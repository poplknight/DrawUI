// DrawUI canvas backend.
//
//	go run .
//
// Listens on :8188. DrawUI_frontend's `pnpm dev` proxies /api and /ws here.
// The process serves the Comfy protocol the canvas needs to open, and runs a
// few non-image nodes so a graph can be queued.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	kindString = "STRING"
	kindInt    = "INT"
)

type inputSpec struct {
	Name       string
	Kind       string
	Optional   bool
	ForceInput bool
	Multiline  bool
	Default    any
	Min        *int64
	Max        *int64
}

type nodeSpec struct {
	Name        string
	Display     string
	Description string
	OutputNode  bool
	Inputs      []inputSpec
	OutputKind  string // empty means no output socket
	run         func(map[string]any) (any, error)
}

type promptNode struct {
	ClassType string         `json:"class_type"`
	Inputs    map[string]any `json:"inputs"`
}

type job struct {
	ID           string
	Status       string
	CreateTime   int64
	StartTime    *int64
	EndTime      *int64
	OutputsCount *int
	ClientID     string
	Prompt       map[string]promptNode
	Outputs      map[string]any
	Error        any
	cancel       context.CancelFunc
}

func (j *job) view() gin.H {
	var start, end any
	if j.StartTime != nil {
		start = *j.StartTime
	}
	if j.EndTime != nil {
		end = *j.EndTime
	}
	var outputs any
	if j.OutputsCount != nil {
		outputs = *j.OutputsCount
	}
	return gin.H{
		"id":                        j.ID,
		"status":                    j.Status,
		"create_time":               j.CreateTime,
		"execution_start_time":      start,
		"execution_end_time":        end,
		"outputs_count":             outputs,
		"previewable_outputs_count": outputs,
		"workflow_id":               nil,
		"execution_error":           j.Error,
		"preview_output":            nil,
	}
}

var catalog = map[string]*nodeSpec{}

func init() {
	min := int64(-1 << 31)
	max := int64(1<<31 - 1)
	register(&nodeSpec{
		Name:        "DrawText",
		Display:     "Text",
		Description: "A text value.",
		Inputs: []inputSpec{{
			Name: "text", Kind: kindString, Multiline: true, Default: "",
		}},
		OutputKind: kindString,
		run: func(in map[string]any) (any, error) {
			return asString(in["text"])
		},
	})
	register(&nodeSpec{
		Name:        "DrawNumber",
		Display:     "Number",
		Description: "An integer value, passed on as text.",
		Inputs: []inputSpec{{
			Name: "value", Kind: kindInt, Default: 0, Min: &min, Max: &max,
		}},
		OutputKind: kindString,
		run: func(in map[string]any) (any, error) {
			n, err := asInt(in["value"])
			if err != nil {
				return nil, err
			}
			return strconv.FormatInt(n, 10), nil
		},
	})
	register(&nodeSpec{
		Name:        "DrawJoin",
		Display:     "Join",
		Description: "Concatenates two text values.",
		Inputs: []inputSpec{
			{Name: "a", Kind: kindString, Multiline: true, Default: ""},
			{Name: "b", Kind: kindString, Multiline: true, Default: "", Optional: true},
		},
		OutputKind: kindString,
		run: func(in map[string]any) (any, error) {
			a, err := asString(in["a"])
			if err != nil {
				return nil, err
			}
			b, _ := asString(in["b"])
			return a + b, nil
		},
	})
	register(&nodeSpec{
		Name:        "DrawShow",
		Display:     "Show",
		Description: "Shows the connected text on the node.",
		OutputNode:  true,
		Inputs: []inputSpec{{
			Name: "text", Kind: kindString, Multiline: true, Default: "",
		}},
		OutputKind: kindString,
		run: func(in map[string]any) (any, error) {
			return asString(in["text"])
		},
	})
}

func register(spec *nodeSpec) {
	catalog[spec.Name] = spec
}

func nodeInfo(spec *nodeSpec) gin.H {
	required := gin.H{}
	optional := gin.H{}
	orderReq := []string{}
	orderOpt := []string{}
	for _, in := range spec.Inputs {
		widget := gin.H{}
		if in.Kind == kindString {
			widget["multiline"] = in.Multiline
			widget["default"] = in.Default
		} else {
			widget["default"] = in.Default
			widget["step"] = 1
			widget["display"] = "number"
			if in.Min != nil {
				widget["min"] = *in.Min
			}
			if in.Max != nil {
				widget["max"] = *in.Max
			}
		}
		if in.ForceInput {
			widget["forceInput"] = true
		}
		entry := []any{in.Kind, widget}
		if in.Optional {
			optional[in.Name] = entry
			orderOpt = append(orderOpt, in.Name)
		} else {
			required[in.Name] = entry
			orderReq = append(orderReq, in.Name)
		}
	}
	input := gin.H{}
	inputOrder := gin.H{}
	if len(orderReq) > 0 {
		input["required"] = required
		inputOrder["required"] = orderReq
	}
	if len(orderOpt) > 0 {
		input["optional"] = optional
		inputOrder["optional"] = orderOpt
	}
	outputs := []string{}
	outputIsList := []bool{}
	if spec.OutputKind != "" {
		outputs = []string{spec.OutputKind}
		outputIsList = []bool{false}
	}
	return gin.H{
		"input":          input,
		"input_order":    inputOrder,
		"is_input_list":  false,
		"output":         outputs,
		"output_is_list": outputIsList,
		"output_name":    outputs,
		"name":           spec.Name,
		"display_name":   spec.Display,
		"description":    spec.Description,
		"python_module":  "draw",
		"category":       "draw",
		"output_node":    spec.OutputNode,
		"search_aliases": []string{},
	}
}

func objectInfo() gin.H {
	out := gin.H{}
	for name, spec := range catalog {
		out[name] = nodeInfo(spec)
	}
	return out
}

type server struct {
	mu       sync.Mutex
	userRoot string
	settings map[string]any
	jobs     []*job
	order    int
	running  int
	pending  int
	queue    chan *job
	clients  map[string]*wsClient
}

type wsClient struct {
	id   string
	conn *websocket.Conn
	mu   sync.Mutex
}

func newServer() *server {
	root := os.Getenv("DRAWUI_USER_DIR")
	if root == "" {
		root = "user"
	}
	root = filepath.Join(root, "default")
	_ = os.MkdirAll(root, 0o755)
	s := &server{
		userRoot: root,
		settings: map[string]any{},
		queue:    make(chan *job, 64),
		clients:  map[string]*wsClient{},
	}
	s.loadSettings()
	go s.worker()
	return s
}

func (s *server) loadSettings() {
	path := filepath.Join(s.userRoot, "comfy.settings.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	_ = json.Unmarshal(raw, &s.settings)
	if s.settings == nil {
		s.settings = map[string]any{}
	}
}

func (s *server) saveSettings() {
	raw, err := json.MarshalIndent(s.settings, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(s.userRoot, "comfy.settings.json"), raw, 0o644)
}

func (s *server) routes(g gin.IRoutes) {
	g.GET("/object_info", s.getObjectInfo)
	g.GET("/object_info/:class", s.getObjectInfoClass)
	g.GET("/system_stats", s.getSystemStats)
	g.GET("/features", s.getFeatures)
	g.GET("/extensions", s.getEmptyList)
	g.GET("/embeddings", s.getEmptyList)
	g.GET("/models", s.getEmptyList)
	g.GET("/models/:folder", s.getEmptyList)
	g.GET("/experiment/models", s.getEmptyList)
	g.GET("/experiment/models/:folder", s.getEmptyList)
	g.GET("/workflow_templates", s.getEmptyList)
	g.GET("/global_subgraphs", s.getEmptyObject)
	g.GET("/users", s.getUsers)
	g.GET("/settings", s.getSettings)
	g.GET("/settings/:id", s.getSetting)
	g.POST("/settings", s.postSettings)
	g.POST("/settings/:id", s.postSetting)
	g.GET("/userdata", s.listUserData)
	g.GET("/userdata/*file", s.handleUserFile)
	g.POST("/userdata/*file", s.handleUserFile)
	g.DELETE("/userdata/*file", s.handleUserFile)
	g.GET("/prompt", s.getPrompt)
	g.POST("/prompt", s.postPrompt)
	g.GET("/queue", s.getQueue)
	g.POST("/queue", s.postQueue)
	g.GET("/history", s.getHistory)
	g.POST("/history", s.postHistory)
	g.POST("/interrupt", s.postInterrupt)
	g.POST("/free", s.ok)
	g.GET("/jobs", s.getJobs)
	g.GET("/jobs/:id", s.getJob)
	g.POST("/jobs/cancel", s.postJobsCancel)
	g.POST("/jobs/:id/cancel", s.postJobCancel)
	g.GET("/default_workflow", s.getDefaultWorkflow)
	g.GET("/ws", s.ws)
}

func (s *server) ok(c *gin.Context) {
	c.Status(http.StatusOK)
}

func (s *server) getEmptyList(c *gin.Context) {
	c.JSON(http.StatusOK, []any{})
}

func (s *server) getEmptyObject(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{})
}

func (s *server) getObjectInfo(c *gin.Context) {
	c.JSON(http.StatusOK, objectInfo())
}

func (s *server) getObjectInfoClass(c *gin.Context) {
	name := c.Param("class")
	spec := catalog[name]
	if spec == nil {
		c.JSON(http.StatusOK, gin.H{})
		return
	}
	c.JSON(http.StatusOK, gin.H{name: nodeInfo(spec)})
}

func (s *server) getSystemStats(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"system": gin.H{
			"os":                          runtime.GOOS,
			"ram_total":                   0,
			"ram_free":                    0,
			"comfyui_version":             "drawui",
			"required_frontend_version":   "1.61.2",
			"installed_templates_version": nil,
			"required_templates_version":  nil,
			"comfy_package_versions":      []any{},
			"python_version":              "go",
			"pytorch_version":             "",
			"embedded_python":             false,
			"deploy_environment":          "local",
			"argv":                        []string{},
		},
		"devices": []gin.H{{
			"name":             "cpu",
			"type":             "cpu",
			"index":            0,
			"vram_total":       0,
			"vram_free":        0,
			"torch_vram_total": 0,
			"torch_vram_free":  0,
		}},
	})
}

func (s *server) getFeatures(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{})
}

func (s *server) getUsers(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"storage":  "server",
		"migrated": true,
	})
}

func (s *server) getSettings(c *gin.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c.JSON(http.StatusOK, s.settings)
}

func (s *server) getSetting(c *gin.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c.JSON(http.StatusOK, s.settings[c.Param("id")])
}

func (s *server) postSettings(c *gin.Context) {
	var incoming map[string]any
	if err := c.ShouldBindJSON(&incoming); err != nil {
		c.Status(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	for k, v := range incoming {
		s.settings[k] = v
	}
	s.saveSettings()
	s.mu.Unlock()
	c.Status(http.StatusOK)
}

func (s *server) postSetting(c *gin.Context) {
	var value any
	if err := c.ShouldBindJSON(&value); err != nil {
		c.Status(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.settings[c.Param("id")] = value
	s.saveSettings()
	s.mu.Unlock()
	c.Status(http.StatusOK)
}

func (s *server) userPath(rel string) (string, error) {
	rel, err := url.PathUnescape(rel)
	if err != nil {
		return "", err
	}
	rel = strings.TrimPrefix(path.Clean("/"+rel), "/")
	if rel == "" || rel == "." || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", errors.New("invalid path")
	}
	return filepath.Join(s.userRoot, filepath.FromSlash(rel)), nil
}

// userdataRel is the encoded path after /userdata/, taken from the raw
// request so a %2F inside a file name stays distinguishable from a real slash.
func userdataRel(c *gin.Context) string {
	raw := c.Request.URL.RequestURI()
	if i := strings.Index(raw, "?"); i >= 0 {
		raw = raw[:i]
	}
	raw = strings.TrimPrefix(raw, "/api")
	raw = strings.TrimPrefix(raw, "/userdata/")
	return strings.TrimPrefix(raw, "/")
}

func (s *server) handleUserFile(c *gin.Context) {
	rel := userdataRel(c)
	if c.Request.Method == http.MethodPost {
		const sep = "/move/"
		if i := strings.Index(rel, sep); i >= 0 {
			s.moveUserData(c, rel[:i], rel[i+len(sep):])
			return
		}
	}
	switch c.Request.Method {
	case http.MethodGet:
		s.getUserData(c, rel)
	case http.MethodPost:
		s.postUserData(c, rel)
	case http.MethodDelete:
		s.deleteUserData(c, rel)
	default:
		c.Status(http.StatusMethodNotAllowed)
	}
}

func (s *server) listUserData(c *gin.Context) {
	dir := c.Query("dir")
	full, err := s.userPath(dir)
	if err != nil {
		c.String(http.StatusBadRequest, "invalid path")
		return
	}
	info, err := os.Stat(full)
	if err != nil || !info.IsDir() {
		c.String(http.StatusNotFound, "Directory not found")
		return
	}
	fullInfo := strings.EqualFold(c.Query("full_info"), "true")
	var out []any
	_ = filepath.Walk(full, func(path string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(full, path)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if fullInfo {
			out = append(out, gin.H{
				"path":     rel,
				"size":     fi.Size(),
				"modified": fi.ModTime().UnixMilli(),
				"created":  fi.ModTime().UnixMilli(),
			})
		} else {
			out = append(out, rel)
		}
		return nil
	})
	if out == nil {
		out = []any{}
	}
	c.JSON(http.StatusOK, out)
}

func (s *server) getUserData(c *gin.Context, rel string) {
	full, err := s.userPath(rel)
	if err != nil {
		c.String(http.StatusBadRequest, "invalid path")
		return
	}
	c.File(full)
}

func (s *server) postUserData(c *gin.Context, rel string) {
	full, err := s.userPath(rel)
	if err != nil {
		c.String(http.StatusBadRequest, "invalid path")
		return
	}
	if strings.EqualFold(c.Query("overwrite"), "false") {
		if _, err := os.Stat(full); err == nil {
			c.String(http.StatusConflict, "file exists")
			return
		}
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.Status(http.StatusBadRequest)
		return
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		c.Status(http.StatusInternalServerError)
		return
	}
	if err := os.WriteFile(full, body, 0o644); err != nil {
		c.Status(http.StatusInternalServerError)
		return
	}
	c.String(http.StatusOK, rel)
}

func (s *server) deleteUserData(c *gin.Context, rel string) {
	full, err := s.userPath(rel)
	if err != nil {
		c.String(http.StatusBadRequest, "invalid path")
		return
	}
	_ = os.Remove(full)
	c.Status(http.StatusOK)
}

func (s *server) moveUserData(c *gin.Context, fromRel, toRel string) {
	from, err := s.userPath(fromRel)
	if err != nil {
		c.String(http.StatusBadRequest, "invalid path")
		return
	}
	to, err := s.userPath(toRel)
	if err != nil {
		c.String(http.StatusBadRequest, "invalid path")
		return
	}
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		c.Status(http.StatusInternalServerError)
		return
	}
	if err := os.Rename(from, to); err != nil {
		c.String(http.StatusNotFound, "file not found")
		return
	}
	c.Status(http.StatusOK)
}

func (s *server) getDefaultWorkflow(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"last_node_id": 0,
		"last_link_id": 0,
		"nodes":        []any{},
		"links":        []any{},
		"groups":       []any{},
		"version":      0.4,
	})
}

func (s *server) queueInfo() gin.H {
	s.mu.Lock()
	defer s.mu.Unlock()
	return gin.H{"exec_info": gin.H{"queue_remaining": s.running + s.pending}}
}

func (s *server) getPrompt(c *gin.Context) {
	c.JSON(http.StatusOK, s.queueInfo())
}

func (s *server) postPrompt(c *gin.Context) {
	var body struct {
		Prompt   map[string]promptNode `json:"prompt"`
		ClientID string                `json:"client_id"`
		PromptID string                `json:"prompt_id"`
		Number   *float64              `json:"number"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Prompt == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":       promptError("no_prompt", "No prompt provided"),
			"node_errors": gin.H{},
		})
		return
	}
	if nodeErrors := validatePrompt(body.Prompt); len(nodeErrors) > 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":       promptError("prompt_outputs_failed_validation", "Prompt outputs failed validation"),
			"node_errors": nodeErrors,
		})
		return
	}
	id := body.PromptID
	if id == "" {
		id = newID()
	}
	now := time.Now().UnixMilli()
	j := &job{
		ID:         id,
		Status:     "pending",
		CreateTime: now,
		ClientID:   body.ClientID,
		Prompt:     body.Prompt,
	}
	s.mu.Lock()
	s.order++
	number := float64(s.order)
	if body.Number != nil {
		number = *body.Number
	}
	s.jobs = append(s.jobs, j)
	s.pending++
	s.mu.Unlock()
	s.queue <- j
	s.broadcast("status", gin.H{"status": s.queueInfo()})
	c.JSON(http.StatusOK, gin.H{
		"prompt_id":   id,
		"number":      number,
		"node_errors": gin.H{},
	})
}

func promptError(kind, message string) gin.H {
	return gin.H{
		"type":       kind,
		"message":    message,
		"details":    message,
		"extra_info": gin.H{},
	}
}

func (s *server) getQueue(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"queue_running": s.jobsByStatus("in_progress"),
		"queue_pending": s.jobsByStatus("pending"),
	})
}

func (s *server) postQueue(c *gin.Context) {
	var body struct {
		Clear  bool     `json:"clear"`
		Delete []string `json:"delete"`
	}
	_ = c.ShouldBindJSON(&body)
	if body.Clear || len(body.Delete) > 0 {
		s.dropPending(body.Clear, body.Delete)
	}
	c.Status(http.StatusOK)
}

func (s *server) getHistory(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{})
}

func (s *server) postHistory(c *gin.Context) {
	c.Status(http.StatusOK)
}

func (s *server) postInterrupt(c *gin.Context) {
	var body struct {
		PromptID string `json:"prompt_id"`
	}
	_ = c.ShouldBindJSON(&body)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.Status == "in_progress" && j.cancel != nil && (body.PromptID == "" || body.PromptID == j.ID) {
			j.cancel()
		}
	}
	c.Status(http.StatusOK)
}

func (s *server) getJobs(c *gin.Context) {
	wanted := map[string]bool{}
	for _, part := range strings.Split(c.Query("status"), ",") {
		if part != "" {
			wanted[part] = true
		}
	}
	limit := 200
	offset := 0
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if v := c.Query("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	s.mu.Lock()
	matched := []*job{}
	for i := len(s.jobs) - 1; i >= 0; i-- {
		j := s.jobs[i]
		if len(wanted) == 0 || wanted[j.Status] {
			matched = append(matched, j)
		}
	}
	s.mu.Unlock()
	total := len(matched)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	views := []any{}
	for _, j := range matched[offset:end] {
		views = append(views, j.view())
	}
	c.JSON(http.StatusOK, gin.H{
		"jobs": views,
		"pagination": gin.H{
			"offset":   offset,
			"limit":    limit,
			"total":    total,
			"has_more": end < total,
		},
	})
}

func (s *server) getJob(c *gin.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.ID == c.Param("id") {
			view := j.view()
			view["outputs"] = j.Outputs
			view["workflow"] = j.Prompt
			c.JSON(http.StatusOK, view)
			return
		}
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
}

func (s *server) postJobCancel(c *gin.Context) {
	s.cancelJob(c.Param("id"))
	c.JSON(http.StatusOK, gin.H{"cancelled": true})
}

func (s *server) postJobsCancel(c *gin.Context) {
	var body struct {
		IDs []string `json:"job_ids"`
	}
	_ = c.ShouldBindJSON(&body)
	for _, id := range body.IDs {
		s.cancelJob(id)
	}
	c.Status(http.StatusOK)
}

func (s *server) cancelJob(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.ID != id {
			continue
		}
		if j.Status == "pending" {
			j.Status = "cancelled"
		}
		if j.Status == "in_progress" && j.cancel != nil {
			j.cancel()
		}
	}
}

func (s *server) dropPending(all bool, ids []string) {
	drop := map[string]bool{}
	for _, id := range ids {
		drop[id] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.Status == "pending" && (all || drop[j.ID]) {
			j.Status = "cancelled"
		}
	}
}

func (s *server) jobsByStatus(status string) []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []any{}
	for _, j := range s.jobs {
		if j.Status == status {
			out = append(out, []any{0, j.ID, j.Prompt, gin.H{}, []string{}})
		}
	}
	return out
}

func (s *server) worker() {
	for j := range s.queue {
		s.mu.Lock()
		if j.Status == "cancelled" {
			if s.pending > 0 {
				s.pending--
			}
			s.mu.Unlock()
			s.broadcast("status", gin.H{"status": s.queueInfo()})
			continue
		}
		if s.pending > 0 {
			s.pending--
		}
		s.running++
		j.Status = "in_progress"
		now := time.Now().UnixMilli()
		j.StartTime = &now
		ctx, cancel := context.WithCancel(context.Background())
		j.cancel = cancel
		s.mu.Unlock()

		s.send(j.ClientID, "status", gin.H{"status": s.queueInfo()})
		s.send(j.ClientID, "execution_start", gin.H{
			"prompt_id": j.ID,
			"timestamp": now,
		})
		outputs, runErr := runPrompt(ctx, j.Prompt, func(id string, output gin.H) {
			s.send(j.ClientID, "executing", gin.H{"node": id, "prompt_id": j.ID})
			s.send(j.ClientID, "executed", gin.H{
				"node":         id,
				"display_node": id,
				"prompt_id":    j.ID,
				"output":       output,
			})
		})
		end := time.Now().UnixMilli()
		s.mu.Lock()
		j.EndTime = &end
		j.cancel = nil
		if s.running > 0 {
			s.running--
		}
		if errors.Is(runErr, context.Canceled) {
			j.Status = "cancelled"
			s.mu.Unlock()
			s.send(j.ClientID, "execution_interrupted", gin.H{
				"prompt_id": j.ID,
				"timestamp": end,
				"node_id":   "",
				"node_type": "",
				"executed":  []string{},
			})
		} else if runErr != nil {
			j.Status = "failed"
			j.Error = gin.H{
				"node_id":           "",
				"node_type":         "",
				"exception_message": runErr.Error(),
				"exception_type":    "RuntimeError",
				"traceback":         []string{runErr.Error()},
			}
			s.mu.Unlock()
			s.send(j.ClientID, "execution_error", gin.H{
				"prompt_id":         j.ID,
				"timestamp":         end,
				"node_id":           nil,
				"node_type":         "",
				"executed":          []string{},
				"exception_message": runErr.Error(),
				"exception_type":    "RuntimeError",
				"traceback":         []string{runErr.Error()},
			})
		} else {
			j.Status = "completed"
			j.Outputs = outputs
			n := len(outputs)
			j.OutputsCount = &n
			s.mu.Unlock()
			s.send(j.ClientID, "executing", gin.H{"node": nil, "prompt_id": j.ID})
			s.send(j.ClientID, "execution_success", gin.H{
				"prompt_id": j.ID,
				"timestamp": end,
			})
		}
		s.broadcast("status", gin.H{"status": s.queueInfo()})
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (s *server) ws(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	id := c.Query("clientId")
	if id == "" {
		id = newID()
	}
	client := &wsClient{id: id, conn: conn}
	s.mu.Lock()
	if old := s.clients[id]; old != nil {
		_ = old.conn.Close()
	}
	s.clients[id] = client
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.clients[id] == client {
			delete(s.clients, id)
		}
		s.mu.Unlock()
		_ = conn.Close()
	}()

	client.send(gin.H{
		"type": "status",
		"data": gin.H{"status": s.queueInfo(), "sid": id},
	})

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &msg) == nil && msg.Type == "feature_flags" {
			client.send(gin.H{"type": "feature_flags", "data": gin.H{}})
		}
	}
}

func (c *wsClient) send(v any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_ = c.conn.WriteJSON(v)
}

func (s *server) send(clientID, event string, data any) {
	msg := gin.H{"type": event, "data": data}
	s.mu.Lock()
	defer s.mu.Unlock()
	if clientID != "" {
		if client := s.clients[clientID]; client != nil {
			client.send(msg)
			return
		}
	}
	for _, client := range s.clients {
		client.send(msg)
	}
}

func (s *server) broadcast(event string, data any) {
	s.send("", event, data)
}

func validatePrompt(prompt map[string]promptNode) gin.H {
	errorsOut := gin.H{}
	for id, node := range prompt {
		spec := catalog[node.ClassType]
		if spec == nil {
			errorsOut[id] = nodeError(node.ClassType, "unknown node "+node.ClassType)
			continue
		}
		for _, in := range spec.Inputs {
			if in.Optional {
				continue
			}
			if _, ok := node.Inputs[in.Name]; ok {
				continue
			}
			if in.Default != nil {
				continue
			}
			errorsOut[id] = nodeError(node.ClassType, "missing input "+in.Name)
		}
	}
	if _, err := executionOrder(prompt); err != nil && len(errorsOut) == 0 {
		errorsOut["prompt"] = nodeError("", err.Error())
	}
	return errorsOut
}

func nodeError(classType, message string) gin.H {
	return gin.H{
		"class_type":        classType,
		"dependent_outputs": []string{},
		"errors": []gin.H{{
			"type":       "exception_during_validation",
			"message":    message,
			"details":    message,
			"extra_info": gin.H{},
		}},
	}
}

func runPrompt(ctx context.Context, prompt map[string]promptNode, onNode func(string, gin.H)) (map[string]any, error) {
	order, err := executionOrder(prompt)
	if err != nil {
		return nil, err
	}
	values := map[string]any{}
	ui := map[string]any{}
	for _, id := range order {
		if err := ctx.Err(); err != nil {
			return ui, err
		}
		node := prompt[id]
		spec := catalog[node.ClassType]
		if spec == nil {
			return ui, fmt.Errorf("unknown node %s", node.ClassType)
		}
		resolved := map[string]any{}
		for _, in := range spec.Inputs {
			raw, ok := node.Inputs[in.Name]
			if !ok {
				resolved[in.Name] = in.Default
				continue
			}
			if from, slot, linked := asLink(raw); linked {
				got, ok := values[from]
				if !ok || slot != 0 {
					return ui, fmt.Errorf("node %s input %s is not available", id, in.Name)
				}
				resolved[in.Name] = got
				continue
			}
			resolved[in.Name] = raw
		}
		out, err := spec.run(resolved)
		if err != nil {
			return ui, fmt.Errorf("node %s: %w", id, err)
		}
		values[id] = out
		text := gin.H{"text": []string{fmt.Sprint(out)}}
		ui[id] = text
		if onNode != nil {
			onNode(id, text)
		}
	}
	return ui, nil
}

func executionOrder(prompt map[string]promptNode) ([]string, error) {
	deps := map[string][]string{}
	consumers := map[string]int{}
	for id := range prompt {
		consumers[id] = 0
	}
	for id, node := range prompt {
		for _, raw := range node.Inputs {
			from, _, linked := asLink(raw)
			if !linked {
				continue
			}
			if _, ok := prompt[from]; !ok {
				return nil, fmt.Errorf("node %s links to missing node %s", id, from)
			}
			deps[id] = append(deps[id], from)
			consumers[from]++
		}
	}
	need := map[string]bool{}
	var stack []string
	for id, node := range prompt {
		spec := catalog[node.ClassType]
		if consumers[id] == 0 || (spec != nil && spec.OutputNode) {
			stack = append(stack, id)
		}
	}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if need[id] {
			continue
		}
		need[id] = true
		stack = append(stack, deps[id]...)
	}
	indeg := map[string]int{}
	forward := map[string][]string{}
	for id := range need {
		indeg[id] = 0
	}
	for id := range need {
		for _, from := range deps[id] {
			indeg[id]++
			forward[from] = append(forward[from], id)
		}
	}
	ready := []string{}
	for id, n := range indeg {
		if n == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	order := []string{}
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		next := append([]string{}, forward[id]...)
		sort.Strings(next)
		for _, nxt := range next {
			indeg[nxt]--
			if indeg[nxt] == 0 {
				ready = append(ready, nxt)
			}
		}
	}
	if len(order) != len(need) {
		return nil, errors.New("prompt contains a cycle")
	}
	return order, nil
}

func asLink(v any) (string, int, bool) {
	arr, ok := v.([]any)
	if !ok || len(arr) < 2 {
		return "", 0, false
	}
	id, ok := arr[0].(string)
	if !ok {
		return "", 0, false
	}
	switch n := arr[1].(type) {
	case float64:
		return id, int(n), true
	case int:
		return id, n, true
	default:
		return "", 0, false
	}
}

func asString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case nil:
		return "", nil
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10), nil
		}
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	default:
		return fmt.Sprint(t), nil
	}
}

func asInt(v any) (int64, error) {
	switch t := v.(type) {
	case float64:
		return int64(t), nil
	case int:
		return int64(t), nil
	case int64:
		return t, nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(t), 10, 64)
	case json.Number:
		return t.Int64()
	default:
		return 0, fmt.Errorf("expected an integer")
	}
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func main() {
	gin.SetMode(gin.ReleaseMode)
	if os.Getenv("DRAWUI_DEBUG") != "" {
		gin.SetMode(gin.DebugMode)
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8188"
	}
	s := newServer()
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	_ = r.SetTrustedProxies(nil)
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Comfy-User")
		c.Header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if c.Request.Method == http.MethodOptions {
			c.Status(http.StatusNoContent)
			c.Abort()
			return
		}
		c.Next()
	})
	s.routes(r)
	s.routes(r.Group("/api"))
	r.GET("/internal/logs", func(c *gin.Context) { c.String(http.StatusOK, "") })
	r.GET("/internal/logs/raw", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"size":    gin.H{"cols": 80, "row": 24},
			"entries": []any{},
		})
	})
	r.NoRoute(func(c *gin.Context) {
		log.Printf("unhandled %s %s", c.Request.Method, c.Request.URL.RequestURI())
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
	})
	log.Printf("drawui listening on :%s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatal(err)
	}
}

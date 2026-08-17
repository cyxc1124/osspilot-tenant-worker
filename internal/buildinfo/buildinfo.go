package buildinfo

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
)

type Info struct {
	Status    string `json:"status,omitempty"`
	Version   string `json:"version"`
	GitTag    string `json:"git_tag,omitempty"`
	GitBranch string `json:"git_branch,omitempty"`
	GitCommit string `json:"git_commit,omitempty"`
	BuildTime string `json:"build_time,omitempty"`
}

func Current() Info {
	tag := strings.TrimSpace(os.Getenv("GIT_TAG"))
	branch := strings.TrimSpace(os.Getenv("GIT_BRANCH"))
	commit := short(os.Getenv("GIT_COMMIT"))
	version := "dev"
	switch {
	case tag != "":
		version = tag
	case branch != "" && commit != "":
		version = branch + "@" + commit
	case branch != "":
		version = branch
	case commit != "":
		version = commit
	}
	return Info{
		Version:   version,
		GitTag:    tag,
		GitBranch: branch,
		GitCommit: commit,
		BuildTime: strings.TrimSpace(os.Getenv("BUILD_TIME")),
	}
}

func Healthz(w http.ResponseWriter, _ *http.Request) {
	info := Current()
	info.Status = "ok"
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(info)
}

func short(commit string) string {
	commit = strings.TrimSpace(commit)
	if len(commit) > 8 {
		return commit[:8]
	}
	return commit
}

package stats

import (
	"strings"
	"time"
)

var periodHours = map[string]int{"24h": 24, "7d": 168, "30d": 720}

const dailyRollupDays = 30

var (
	putActions    = map[string]struct{}{"upload": {}}
	getActions    = map[string]struct{}{"download": {}, "preview": {}, "version_download": {}, "share_access": {}}
	deleteActions = map[string]struct{}{"delete": {}, "purge": {}, "version_delete": {}}
	excludedActs  = map[string]struct{}{"login_failed": {}, "password_change": {}}
	adminActions  = map[string]struct{}{
		"user_create": {}, "user_delete": {}, "user_update": {}, "modify_user_role": {},
		"create_tenant": {}, "delete_tenant": {}, "disable_tenant": {}, "enable_tenant": {}, "update_tenant": {},
		"modify_quota": {}, "bucket_create": {}, "bucket_import": {}, "bucket_delete": {}, "modify_bucket": {},
		"modify_bucket_quota": {}, "modify_access_logging": {}, "modify_bucket_policy": {}, "modify_bucket_cors": {},
		"modify_permission": {}, "modify_lifecycle": {}, "force_unlock_file": {},
		"restart_rgw": {}, "rolling_restart_rgw": {}, "update_system_settings": {},
		"request_tenant_api_access": {}, "approve_tenant_api_access": {}, "reject_tenant_api_access": {},
		"disable_tenant_api_access": {}, "create_application": {}, "update_application": {}, "delete_application": {},
		"create_access_key": {}, "disable_access_key": {}, "issue_sts_credentials": {},
	}
)

type counter struct {
	uploadBytes, downloadBytes int64
	requestCount, getCount     int64
	putCount, deleteCount      int64
	errorCount                 int64
	active                     map[int64]struct{}
}

type userCounter struct {
	username                                *string
	uploadCount, downloadCount, deleteCount int64
	accessCount, uploadBytes, downloadBytes int64
}

func isObjectRequest(action string) bool {
	_, admin := adminActions[action]
	_, skip := excludedActs[action]
	return !admin && !skip
}

func firstLevelPrefix(key string) string {
	if key == "" || strings.HasPrefix(key, ".trash/") {
		return ""
	}
	i := strings.IndexByte(key, '/')
	if i < 0 {
		return ""
	}
	return key[:i+1]
}

func (c *counter) add(action, status string, size int64, actor *int64) {
	c.requestCount++
	if status != "success" {
		c.errorCount++
	}
	if _, ok := getActions[action]; ok {
		c.getCount++
		c.downloadBytes += size
	} else if _, ok := putActions[action]; ok {
		c.putCount++
		c.uploadBytes += size
	} else if _, ok := deleteActions[action]; ok {
		c.deleteCount++
	}
	if actor != nil {
		if c.active == nil {
			c.active = map[int64]struct{}{}
		}
		c.active[*actor] = struct{}{}
	}
}

func (c *userCounter) add(action string, size int64, key string) {
	if _, ok := putActions[action]; ok {
		c.uploadCount++
		c.uploadBytes += size
	} else if _, ok := getActions[action]; ok {
		c.downloadCount++
		c.downloadBytes += size
	} else if _, ok := deleteActions[action]; ok {
		c.deleteCount++
	}
	if key != "" {
		if _, ok := putActions[action]; ok {
			c.accessCount++
		} else if _, ok := getActions[action]; ok {
			c.accessCount++
		}
	}
}

func inWindow(at, now time.Time, hours int) bool {
	return !at.Before(now.Add(-time.Duration(hours) * time.Hour))
}

func rfc3339(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

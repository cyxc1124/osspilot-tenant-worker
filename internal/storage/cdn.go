package storage

import (
	"net/url"
	"strings"
)

// rewriteCDN replaces the URL host/scheme with the CDN base when set.
// ponytail: only host rewrite; path/query stay as signed by RGW.
func rewriteCDN(raw, cdnBase string) string {
	cdnBase = strings.TrimRight(strings.TrimSpace(cdnBase), "/")
	if raw == "" || cdnBase == "" {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	base, err := url.Parse(cdnBase)
	if err != nil || base.Host == "" {
		return raw
	}
	u.Scheme = base.Scheme
	u.Host = base.Host
	return u.String()
}

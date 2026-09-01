#!/bin/sh
# GitLab Kaniko 作业入口。逻辑放在文件里，避免 CI YAML 被 eval 时把 $( / \( 拆坏。
set -eu

mkdir -p /kaniko/.docker

gitlab_image="${CI_REGISTRY_IMAGE:?}"
dest="--destination ${gitlab_image}:${CI_COMMIT_SHORT_SHA:?}"
if [ -n "${CI_COMMIT_TAG:-}" ]; then
	dest="${dest} --destination ${gitlab_image}:${CI_COMMIT_TAG}"
else
	dest="${dest} --destination ${gitlab_image}:${CI_COMMIT_REF_SLUG:?}"
fi
# :latest 跟随 main，不跟默认分支 develop
if [ "${CI_COMMIT_BRANCH:-}" = "main" ]; then
	dest="${dest} --destination ${gitlab_image}:latest"
fi

printf '{"auths":{"%s":{"username":"%s","password":"%s"}}}\n' \
	"${CI_REGISTRY:?}" "${CI_REGISTRY_USER:?}" "${CI_REGISTRY_PASSWORD:?}" \
	>/kaniko/.docker/config.json

echo "Destinations:${dest}"

# 内网 Registry 不走代理，否则 push GitLab 会失败
_internal="localhost,127.0.0.1,gitlab.cyxc.club,registry.gitlab.cyxc.club,.cyxc.club"
if [ -n "${HTTP_PROXY:-}${HTTPS_PROXY:-}${http_proxy:-}${https_proxy:-}${ALL_PROXY:-}${all_proxy:-}" ]; then
	if [ -n "${NO_PROXY:-}" ]; then
		NO_PROXY="${NO_PROXY},${_internal}"
	else
		NO_PROXY="${_internal}"
	fi
	no_proxy="${NO_PROXY}"
	export NO_PROXY no_proxy
fi

proxy_args=""
for name in HTTP_PROXY HTTPS_PROXY NO_PROXY ALL_PROXY http_proxy https_proxy no_proxy all_proxy; do
	eval "val=\${${name}:-}"
	[ -z "${val}" ] && continue
	proxy_args="${proxy_args} --build-arg ${name}=${val}"
done
if [ -n "${proxy_args}" ]; then
	echo "Passing proxy build-args"
else
	echo "No proxy env, build without proxy"
fi

git_tag=""
if [ -n "${CI_COMMIT_TAG:-}" ]; then
	git_tag="${CI_COMMIT_TAG}"
fi

# shellcheck disable=SC2086
exec /kaniko/executor \
	--context "${CI_PROJECT_DIR:?}" \
	--dockerfile "${CI_PROJECT_DIR}/Dockerfile" \
	--build-arg "GIT_TAG=${git_tag}" \
	--build-arg "GIT_COMMIT=${CI_COMMIT_SHA:?}" \
	${proxy_args} \
	--cache=true \
	--cache-repo="${gitlab_image}/cache" \
	--compressed-caching=false \
	${dest}

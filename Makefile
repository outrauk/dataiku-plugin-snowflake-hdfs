PLUGIN_VERSION := $(shell node -p "require('./plugin.json').version")
PLUGIN_ID := $(shell node -p "require('./plugin.json').id")

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json custom-recipes code-env python-lib README.md LICENSE

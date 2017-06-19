.PHONY: test

SA:=source activate
ENV:=spylon-kernel-dev
SHELL:=/bin/bash

help:
# http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

activate: ## Make a conda activate command for eval `make activate`
	@echo "$(SA) $(ENV)"

clean: ## Make a clean source tree
	-rm -rf dist
	-rm -rf build
	-rm -rf *.egg-info
	-find . -name __pycache__ -exec rm -fr {} \;

env: ## Make a conda dev environment
	conda create -y -n $(ENV) python=3.6 notebook
	source activate $(ENV) && \
		pip install -r requirements.txt -r requirements-test.txt -e . && \
		python -m spylon_kernel install --sys-prefix

notebook: ## Make a development notebook
	$(SA) $(ENV) && jupyter notebook --notebook-dir=examples/ \
		--no-browser \
		--NotebookApp.token=''

nuke: ## Make clean + remove conda env
	-conda env remove -n $(ENV) -y

sdist: ## Make a source distribution
	$(SA) $(ENV) && python setup.py sdist

release: clean ## Make a pypi release of a tagged build
	$(SA) $(ENV) && python setup.py sdist register upload

test: ## Make a test run
	$(SA) $(ENV) && python run_tests.py -vxrs --color=yes

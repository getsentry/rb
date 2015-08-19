setup-git:
	@echo "--> Installing git hooks"
	@pip install flake8
	@cd .git/hooks && ln -sf ../../hooks/* ./

test:
	@py.test -vv --tb=short

setup-git:
	@echo "--> Installing git hooks"
	@pip install flake8
	cd .git/hooks && ln -sf ../../hooks/* ./

test:
	PYTHONPATH=. py.test -vv --tb=short

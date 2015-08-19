# Install packages with brew
brew update >/dev/null
brew outdated pyenv || brew upgrade --quiet pyenv
brew outdated redis || brew install --quiet redis

# Install required python version for this build
pyenv install -ks $PYTHON_VERSION
pyenv global $PYTHON_VERSION
python --version

# Avoid lengthy brew cleanup process
export HOMEBREW_NO_INSTALL_CLEANUP=1

# Install packages with brew
brew update >/dev/null
brew outdated pyenv || brew upgrade --quiet pyenv

# Instlal redis always
brew install --quiet redis

# Install required python version for this build
pyenv install -ks $PYTHON_VERSION
pyenv global $PYTHON_VERSION
python --version

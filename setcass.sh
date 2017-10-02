#!/usr/bin/bash
wget https://bootstrap.pypa.io/get-pip.py
python get-pip.py --user
exploit PATH=$PATH:$HOME/.local/bin
if [[ ! -d '$HOME/mypylib' ]]; then
	cd ~
	mkdir mypylib
fi
pip install --prefix=$HOME/mypylib cassandra-driver

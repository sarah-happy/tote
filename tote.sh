#!/bin/sh
export PYTHONPATH="$HOME/tote:$PYTHONPATH"
exec python3 -m tote "$@"

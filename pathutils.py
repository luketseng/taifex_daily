#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, sys, logging, inspect

_logger = logging.getLogger(__name__)

def sys_path_append(path):
    """
    Add a new path to the Python module search path.
    The path can also be relative to the module calling this function.
    """

    abspath = path
    if not os.path.isabs(path):
        caller_file = inspect.getfile(inspect.stack()[1][0])
        if os.path.isabs(caller_file):
            basedir = os.path.dirname(caller_file)
        else: # called by a main script
            basedir = sys.path[0]
        abspath = os.path.normpath(os.path.join(basedir, path))

    append = abspath not in sys.path
    _logger.info("Tryingdd path '%s' -> '%s' to the module search path; append = %s, sys.path = %s", path, abspath, append, sys.path)

    if append: sys.path.append(abspath)

def abspath(relpath, module=None):
    if os.path.isabs(relpath): return relpath

    caller_file = module.__file__ if module else inspect.getfile(inspect.stack()[1][0])
    if os.path.isabs(caller_file):
        basedir = os.path.dirname(caller_file)
    else: # called by a main script
        basedir = sys.path[0]

    return os.path.normpath(os.path.join(basedir, relpath))

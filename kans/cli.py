from __future__ import annotations
import argparse
import sys
from typing import Sequence
import platform

from kans import __version__


def main(argv: Sequence[str] | None = None) -> int:
    """
    Command-line implementation of maellin that executes the main bit of the application.
    Args:
    argv (Sequence[str]): The arguments to be passed to the application for parsing.
        Defaults to None.
    """
    print(
        r"""
        Welcome to
        KANS.IO
        The easiest way to author data workflows with minimal setup!
        Maellin.io verison %s
        Using Python version %s (%s, %s)""" % (
            __version__,
            platform.python_version(),
            platform.python_build()[0],
            platform.python_build()[1]))

    if argv is None:
        argv = sys.argv[1:]

    # app.run(argv)
    return  # app.exit_code()
#!/bin/bash
set -x
set -v
arch -arch arm64e /Applications/Xcode.app/Contents/SharedFrameworks/LLDB.framework/Resources/debugserver "$@"

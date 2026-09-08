#!/bin/sh
# The templates in web/, turned into the Rill that renders them.
#
#     sh tools/pages.sh
#
# What comes out is checked in, so building the broker is `rill build` and
# nothing else. Run this after changing a template.
set -e
cd "$(dirname "$0")/.."
GEN="${TMPLGEN:-/tmp/tmplgen}"
if [ ! -x "$GEN" ]; then
  rill build "${RILL_SRC:-$HOME/source/funclang}/tools/tmplgen.rill" -o "$GEN"
fi
for t in head foot overview people; do
  "$GEN" "web/$t.tmpl" "src/$t.rill"
done

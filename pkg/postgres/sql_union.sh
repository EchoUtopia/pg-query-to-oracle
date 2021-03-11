#!/usr/bin/env bash

# Trigger this script by running `make generate PKG=./pkg/sql/parser` from the
# repository root to ensure your PATH includes vendored binaries.

set -euo pipefail

# This step runs sql.y through the YACC compiler directly without
# performing any type/token declaration modifications, throwing out
# the result on a successful compilation. This allows the YACC compiler
# to check for any type clashes between type declarations and rule
# constructions. Note that this step does not require any modification
# to union type accessors in the Go code within rules, as the YACC
# compiler does not type check Go code.
function type_check() {
    ret=$(goyacc -o /dev/null -p sql sql.y)
    ! echo "$ret" | grep -F 'conflicts'
}

# This step performs the actual compilation from the sql.y syntax to
# the sql.go parser implementation. It first translates the syntax
# file to use the union type for any type declaration whose type was
# given a union accessor on sqlSymUnion. Next, it performs the YACC
# compilation.
function compile() {
    # Determine the types that will be migrated to union types by looking
    # at the accessors of sqlSymUnion. The first step in this pipeline
    # prints every return type of a sqlSymUnion accessor on a separate line.
    # The next step regular expression escapes these types. The third step
    # joins all of the lines into a single line with a '|' character to be
    # used as a regexp "or" meta-character. Finally, the last '|' character
    # is striped from the string.
    TYPES=$(awk '/func.*sqlSymUnion/ {print $(NF - 1)}' sql.y | \
        sed -e 's/[]\/$*.^|[]/\\&/g' | \
        tr '\n' '|' | \
        sed -E '$s/.$//')

    # We want a file named "sql.y" to be compiled by the YACC compiler so that
    # the comments in the generated file will reflect where the syntax came
    # from. This is why we move the original sql.y file around.
    mv sql.y sql.y.tmp

    # Migrate the original syntax file, with the types determines above being
    # replaced with the union type in their type declarations.
    sed -E "s_(type|token) <($TYPES)>_\1 <union> /* <\2> */_" sql.y.tmp > sql.y;

    # Compile this new "unionized" syntax file through YACC, this time writing
    # the output to sql.go.
    ret=$(goyacc -o sql.go -p sql sql.y)
    ! echo "$ret" | grep -F 'conflicts'

    # Overwrite the migrated syntax file with the original syntax file.
    mv sql.y.tmp sql.y

    # Add a comment to the top of the autogenerated sql.go file. These make
    # github and (respectively) reviewable recognize the file as autogenerated.
    echo -e '// Code generated by goyacc.\n// GENERATED FILE DO NOT EDIT' | \
        cat - sql.go > sql.go.tmp && mv -f sql.go.tmp sql.go
}

echo 'Type checking sql.y'
type_check

echo 'Compiling sql.go'
compile

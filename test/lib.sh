# What every test script does before it believes a broker is there.
#
# A broker that cannot have its port exits non-zero, but a script that starts
# one in the background and looks away still has to look. And looking is not
# enough on its own: if something else is already on that port then the port
# answers, and every check that follows is asking questions of a stranger — a
# broker with another `dir`, another `peers`, another idea of which node it
# is. That reads as a message queue losing messages, and it cost an afternoon
# before it was understood. So this asks the harder question: is the process
# listening on that port the one we just started?
#
#     ./rillmq 9801 "dir=$D" >/dev/null 2>&1 &
#     up 9801 $! || exit 1

up() {
  port="$1"
  pid="$2"
  i=0
  while [ "$i" -lt 60 ]; do
    who=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null | tr '\n' ' ')
    case " $who " in
      *" $pid "*) return 0 ;;
    esac
    if [ -n "$who" ]; then
      printf 'port %s is held by pid(s) %s, not by the broker just started (%s)\n' \
        "$port" "$who" "$pid" >&2
      return 1
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      printf 'the broker for port %s died before it listened\n' "$port" >&2
      return 1
    fi
    sleep 0.1
    i=$((i + 1))
  done
  printf 'the broker on port %s never listened\n' "$port" >&2
  return 1
}

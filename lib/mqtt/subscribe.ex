defmodule Mqtt.Subscribe do
  defstruct topics: [], msg_id: :undefined, dup: false, qos: :at_least_once, extra: ""
end

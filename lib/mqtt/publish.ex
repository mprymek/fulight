defmodule Mqtt.Publish do
  defstruct topic: "", msg_id: :undefined, dup: false, qos: :at_most_once, retain: false, payload: ""
end

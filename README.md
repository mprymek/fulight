Fulight
=======

Simple MQTT client for Elixir based on https://github.com/refuge/fubar code.
It's just a thin wrapper around Fubar currently fully supporting CONNECT, SUBSCRIBE, UNSUBSCRIBE and PUBLISH messages.


You can write your MQTT client in Elixir like this:

```elixir
defmodule Test do
  require Logger
  use Fulight.Client

  @regname __MODULE__

  def start_link do
    Logger.info "#{__MODULE__} starting"

    opts = [
      host: {127,0,0,1},
      username: "",
      password: "",
      client_id: Atom.to_string(__MODULE__),
      transport: :ranch_tcp,
      handler: __MODULE__,
      handler_state: :some_state_here,
    ]
    Fulight.Client.start_link @regname, opts
  end

  def handle_connected(state) do
    Logger.info "Mqtt connect."
    msend @regname, %Mqtt.Subscribe{topics: ["/test/#"]}
    #msend @regname, %Mqtt.Unsubscribe{topics: ["/test/#"]}
    state
  end

  def handle_message(state,%Mqtt.Publish{topic: topic, payload: pl}) do
    Logger.info "#{topic} = #{inspect pl}"
    msend @regname, %Mqtt.Publish{topic: "repeat/#{topic}", payload: pl}
    state
  end

  def handle_message(state,msg) do
    Logger.debug "Unhandled mqtt message. state=#{inspect state} msg=#{inspect msg}"
    state
  end

end

```

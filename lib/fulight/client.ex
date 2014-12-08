defmodule Fulight.Client.State do
  defstruct handler: nil, hstate: nil, regname: nil
end

defmodule Fulight.Client do
  alias Fulight.Client.State
  @behaviour :mqtt_client

  defmacro __using__ _opts do
    quote do
      require Logger
      @behaviour :mqtt_client

      defmacro msend(regname,msg) do
        quote do
          send(unquote(regname), Fulight.Client.to_record(unquote(msg)))
        end
      end

      def handle_connected(state) do
        Logger.debug "Unhandled mqtt connect. state=#{inspect state}"
        state
      end

      def handle_disconnected(state) do
        Logger.debug "Unhandled mqtt disconnect. state=#{inspect state}"
        state
      end

      def handle_message(state,msg) do
        Logger.debug "Unhandled mqtt message. state=#{inspect state} msg=#{inspect msg}"
        state
      end

      def handle_event(state,event) do
        Logger.debug "Unhandled mqtt event. state=#{inspect state} event=#{inspect event}"
        state
      end

      defoverridable [handle_connected: 1, handle_disconnected: 1, handle_message: 2, handle_event: 2]
    end
  end

  def start_link regname, opts do
    handler = opts[:handler]
    handler_state = opts[:handler_state]
    opts = Dict.merge opts, [handler: __MODULE__, handler_state: %State{regname: regname, handler: handler, hstate: handler_state}]
    {:ok, client} = :mqtt_client.start_link(opts)
    Process.register client, regname
    {:ok, client}
  end

  def handle_connected(s=%State{handler: h, hstate: hs}) do
    hs = hs |> h.handle_connected
    %State{s|hstate: hs}
  end

  def handle_disconnected(s=%State{handler: h, hstate: hs}) do
    hs = hs |> h.handle_disconnected
    %State{s|hstate: hs}
  end

  def handle_message(msg, s=%State{handler: h, hstate: hs}) do
    msg = msg |> from_record
    hs = hs |> h.handle_message(msg)
    %State{s|hstate: hs}
  end

  def handle_event(event, s=%State{handler: h, hstate: hs}) do
    hs = hs |> h.handle_event(event)
    %State{s|hstate: hs}
  end

  def to_record(%Mqtt.Publish{topic: topic, msg_id: msg_id, dup: dup, qos: qos, retain: retain, payload: payload}), do:
    {:mqtt_publish, topic, msg_id, dup, qos, retain, payload}

  def to_record(%Mqtt.Subscribe{topics: topics, msg_id: msg_id, dup: dup, qos: qos, extra: extra}), do:
    {:mqtt_subscribe, msg_id, topics, dup, qos, extra}

  def to_record(%Mqtt.Unsubscribe{topics: topics, msg_id: msg_id, dup: dup, qos: qos, extra: extra}), do:
    {:mqtt_unsubscribe, msg_id, topics, dup, qos, extra}

  def to_record(x), do: x

  #############################################################################
  # private

  defp from_record({:mqtt_publish, topic, msg_id, dup, qos, retain, payload}), do:
    %Mqtt.Publish{topic: topic, msg_id: msg_id, dup: dup, qos: qos, retain: retain, payload: payload}

  defp from_record({:mqtt_subscribe, msg_id, topics, dup, qos, extra}), do:
    %Mqtt.Subscribe{topics: topics, msg_id: msg_id, dup: dup, qos: qos, extra: extra}

  defp from_record({:mqtt_unsubscribe, msg_id, topics, dup, qos, extra}), do:
    %Mqtt.Unsubscribe{topics: topics, msg_id: msg_id, dup: dup, qos: qos, extra: extra}

  defp from_record(x), do: x

end

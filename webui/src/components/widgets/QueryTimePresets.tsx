import { For } from "solid-js";

interface QueryTimePresetsProps {
  range: string;
  setRange: (value: string) => void;
}

const presets = ["5m", "15m", "1h", "6h", "24h"];
presets.forEach((it) => {
  if (it.startsWith("-")) throw Error("presets values are invalid, none of them must be negative");
});

export default function QueryTimePresets(props: QueryTimePresetsProps) {
  return (
    <>
      <span class="pr-ds-4">Time</span>
      <For each={presets}>
        {(preset) => (
          <button
            class={[
              "h-7 rounded-6 border border-gray-6 px-ds-10 text-16 hover:bg-gray-3",
              {
                "border-dark-ring bg-dark-accent text-dark-accent-foreground":
                  props.range === `-${preset}`,
              },
            ]}
            aria-pressed={props.range === `-${preset}` ? "true" : "false"}
            onClick={() => props.setRange(`-${preset}`)}
          >
            {preset}
          </button>
        )}
      </For>
    </>
  );
}

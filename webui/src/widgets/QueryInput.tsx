import type { Component, JSX } from 'solid-js';

const QueryInput: Component = () => {
    return (
        <div class="relative flex min-w-60 flex-auto">
            <input
                placeholder='{region-eu-west-1} level=error'
                class="flex min-h-12 min-w-60 flex-auto items-center gap-2 border border-border bg-input px-2.5 max-[640px]:flex-wrap max-[640px]:py-2"
            >
            </ input>
        </div>
    );
};

export default QueryInput;

interface QueryEditorProps {
  query: string;
  setQuery: (value: string) => void;

  setInputRef: (inpput: HTMLInputElement) => void;
}

export default function QueryEditor(props: QueryEditorProps) {
  return (
    <label class="flex h-full min-w-0 flex-1 items-center gap-ds-16 rounded-6 border border-gray-6 bg-bg px-ds-12 focus-within:border-dark-ring">
      <span>LOQL</span>
      <input
        ref={props.setInputRef}
        value={props.query}
        onInput={(event) => props.setQuery(event.currentTarget.value.trim())}
        aria-label="LOQL query"
        class="min-w-0 flex-1 bg-transparent font-mono text-13 text-gray-12 outline-none! placeholder:text-gray-11"
        placeholder="Write a query"
        spellcheck="false"
        autocomplete="off"
      />
      <kbd class="rounded-2 border border-gray-6 px-ds-6 font-sans">a</kbd>
    </label>
  );
}

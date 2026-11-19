export default function Keycap(props: { label: string }) {
  return (
    <kbd class="flex h-[22px] min-w-[22px] items-center justify-center rounded-4 border border-b-2 border-gray-6 bg-gray-2 px-ds-4 font-sans text-12 leading-16 font-medium group-enabled:border-keycap-primary-border group-enabled:bg-keycap-primary-bg group-enabled:text-keycap-primary-text">
      {props.label}
    </kbd>
  );
}

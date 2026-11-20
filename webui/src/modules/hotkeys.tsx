import {
  AppHotkeysStore,
  detectPlatform,
  formatForDisplay,
  getAppHotkeyRegistrationOptions,
  getHotkeyManager,
  publishAppHotkeysStore,
  resolveAppHotkey,
  type AppHotkey,
  type AppHotkeys,
  type CustomHotkeys,
  type HotkeyCommonOptions,
  type HotkeyPlatform,
} from "@tanstack/hotkeys";
import {
  createContext,
  createMemo,
  createSignal,
  onSettled,
  untrack,
  useContext,
  type ParentProps,
} from "solid-js";

const HOTKEYS_KEY = "ochi.hotkeys";

interface HotkeysContextValue {
  store: AppHotkeysStore;
  scope: string;
  platform: HotkeyPlatform;
}
const HotkeysContext = createContext<HotkeysContextValue | null>(null);

export function browserPlatform(): HotkeyPlatform {
  if (typeof navigator === "undefined") return "linux";
  const client = navigator as Navigator & { userAgentData?: { platform?: string } };
  // Client hints are not supported by Safari/Firefox; iPadOS may identify as macOS.
  const agent = client.userAgentData?.platform || navigator.userAgent;
  return /Mac|iPhone|iPad|iPod/i.test(agent) ? "mac" : detectPlatform(agent);
}

export function loadCustomHotkeys(): CustomHotkeys {
  try {
    return JSON.parse(localStorage.getItem(HOTKEYS_KEY) ?? "{}");
  } catch (err) {
    console.error("failed to load custom hotkey", err);
    return {};
  }
}

export function saveCustomHotkeys(custom: CustomHotkeys) {
  try {
    localStorage.setItem(HOTKEYS_KEY, JSON.stringify(custom));
  } catch (err) {
    console.error("failed to save custom hotkey", err);
  }
}

export function HotkeysProvider(
  props: ParentProps<{
    scope: string;
    exclusive?: boolean;
    appHotkeys?: AppHotkeys;
    customHotkeys?: CustomHotkeys;
    onCustomHotkeysChange?: (custom: CustomHotkeys) => void;
  }>,
) {
  const parent = useContext(HotkeysContext);
  const platform = browserPlatform();
  const store = untrack(() =>
    props.appHotkeys
      ? new AppHotkeysStore(props.appHotkeys, {
          platform,
          customHotkeys: props.customHotkeys,
          onCustomHotkeysChange: props.onCustomHotkeysChange,
        })
      : parent!.store,
  );

  const context = { store, scope: untrack(() => props.scope), platform };
  onSettled(() => {
    const release = getHotkeyManager().activateScope(props.scope, {
      exclusive: props.exclusive,
      parent: parent?.scope,
    });
    const unpublish = props.appHotkeys ? publishAppHotkeysStore(store) : undefined;
    return () => {
      release();
      unpublish?.();
    };
  });
  return <HotkeysContext value={context}>{props.children}</HotkeysContext>;
}

export function useHotkey(
  hotkey: AppHotkey,
  callback: () => void,
  options: HotkeyCommonOptions = {},
) {
  const context = useContext(HotkeysContext)!;

  onSettled(() => {
    let unregister: (() => void) | undefined;

    const register = () => {
      unregister?.();
      unregister = undefined;

      // app hotkeys come from defined appHotkeys
      const appKey = resolveAppHotkey(hotkey, context.store);
      if (!appKey.hotkey) {
        console.error("attempt to use unregistered hotkey", hotkey);
        return;
      }

      const handle = getHotkeyManager().register(appKey.hotkey, callback, {
        target: window,
        platform: context?.platform ?? browserPlatform(),
        scope: context?.scope,
        ...getAppHotkeyRegistrationOptions(appKey),
        ...options,
      });
      unregister = handle.unregister;
    };

    register();
    const unsubscribe = context?.store.store.subscribe(register);

    return () => {
      unsubscribe?.unsubscribe();
      unregister?.();
    };
  });
}

export function useHotkeyLabel(hotkey: AppHotkey) {
  const context = useContext(HotkeysContext)!;
  const state = useStoreState(context.store.store);

  return createMemo(() => {
    const keys = state().appHotkeys[hotkey.id]?.hotkey;

    if (!keys) {
      console.warn("no keys bind found");
      return "";
    }
    return formatForDisplay(keys, { platform: context.platform });
  });
}

export function useAppHotkeyList() {
  const context = useContext(HotkeysContext)!;
  const app = useStoreState(context.store.store);

  return createMemo(() => Object.values(app().appHotkeys).map((appHotkey) => ({ appHotkey })));
}

function useStoreState<T>(store: {
  state: T;
  subscribe: (fn: () => void) => { unsubscribe: () => void };
}) {
  const [state, setState] = createSignal<T>(() => store.state);
  onSettled(() => {
    setState(() => store.state);
    const subscription = store.subscribe(() => setState(() => store.state));
    return () => subscription.unsubscribe();
  });
  return state;
}

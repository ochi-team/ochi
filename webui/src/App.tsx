import { Title } from "@solidjs/meta";
import { Loading } from "solid-js";
import { Router } from "./router";
import "./App.css";

import { HotkeysProvider } from "./modules/hotkeys";
import { appHotkeys } from "./modules/keymap";
import { loadCustomHotkeys, saveCustomHotkeys } from "./modules/hotkeys";

export default function App() {
  return (
    <HotkeysProvider
      scope="global"
      appHotkeys={appHotkeys}
      customHotkeys={loadCustomHotkeys()}
      onCustomHotkeysChange={saveCustomHotkeys}
    >
      <Router>
        {(props) => (
          <>
            <Title>Ochi logs viewer</Title>
            <Loading fallback={<main class="px-4 py-12">Loading…</main>}>{props.children}</Loading>
          </>
        )}
      </Router>
    </HotkeysProvider>
  );
}

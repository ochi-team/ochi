import { createRouter, intentPreload } from "@solidjs/router";
import { lazy } from "solid-js";

// Link preloading is opt-in. intentPreload() warms a link's code and runs
// its preload when the pointer rests on it (or on focus/touch).
export const Router = createRouter({
  routes: [{ path: "/", component: lazy(() => import("./routes/index")) }],
  preloadLinks: intentPreload(),
});

export const { paths } = Router;

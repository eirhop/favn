# Favn Web

SvelteKit browser edge/BFF prototype for Favn. The web tier owns browser sessions and relays operator requests to the private orchestrator API.

## Creating a project

If you're seeing this, you've probably already done this step. Congrats!

```sh
# create a new project
npx sv create my-app
```

To recreate this project with the same configuration:

```sh
# recreate this project
npx sv@0.15.1 create --template minimal --types ts --add prettier eslint vitest="usages:unit,component" playwright tailwindcss="plugins:typography,forms" sveltekit-adapter="adapter:auto" storybook mcp="ide:opencode" --install npm favn_web
```

## Developing

Once you've created a project and installed dependencies with `npm install` (or `pnpm install` or `yarn`), start a development server:

```sh
npm run dev

# or start the server and open the app in a new browser tab
npm run dev -- --open
```

### Environment

Create a local `.env` file in `web/favn_web` when running the prototype directly:

```sh
FAVN_ORCHESTRATOR_BASE_URL=http://127.0.0.1:4101
FAVN_ORCHESTRATOR_SERVICE_TOKEN=change-me
FAVN_WEB_SESSION_SECRET=replace-with-a-long-random-secret

# Optional prototype fallback login handled by the web tier.
FAVN_WEB_ADMIN_USERNAME=admin
FAVN_WEB_ADMIN_PASSWORD=admin-password
# Optional, defaults to 28800 seconds (8 hours).
FAVN_WEB_ADMIN_SESSION_TTL_SECONDS=28800
```

The preferred login path is orchestrator-owned username/password auth and it is attempted before any web-local fallback. The `FAVN_WEB_ADMIN_*` credentials are only a prototype web-local UI shell fallback for direct local runs. A web-local fallback session can render the shell, but it cannot access real orchestrator control-plane data unless the same credentials also authenticate successfully with the orchestrator and produce an orchestrator-owned session.

### Storybook

```sh
npm run storybook
```

## Building

To create a production version of your app:

```sh
npm run build
```

You can preview the production build with `npm run preview`.

> To deploy your app, you may need to install an [adapter](https://svelte.dev/docs/kit/adapters) for your target environment.

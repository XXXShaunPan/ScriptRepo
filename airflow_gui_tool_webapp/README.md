# Lovito Task Hub SeaTalk WebApp

SeaTalk WebApp version of the original Electron Airflow GUI Tool. The UI stays close to the desktop task hub, while Airflow login, cookies, and write permissions now live on the server.

## What Changed

- `public/` serves the WebApp UI.
- `server/` exposes Express REST APIs under `/api/*`.
- `src/airflowClient.js` still talks to Airflow, but only from the server.
- SeaTalk SSO signs users in with `getSSOToken()` from the vendored Web SDK browser bundle in `public/vendor/`.
- Admin-only actions are controlled by `SEATALK_ADMIN_EMPLOYEE_CODES`.
- The Add DAG modal downloads generated `.py` files instead of writing to a local folder.
- Desktop-only auto-update and Electron notification flows are removed.

## Local Development

```bash
cd /Users/shaun.pan/shaun/projects_sync_to_github/airflow_gui_tool_webapp
npm install
cp .env.example .env
npm run dev
```

Open `http://127.0.0.1:5178`.

When `ALLOW_DEV_AUTH=true`, a normal browser session logs in as `DEV_USER_EMPLOYEE_CODE`. Disable this in production.

## Production Env

Required:

```env
SESSION_SECRET=...
SEATALK_APP_ID=...
SEATALK_APP_SECRET=...
SEATALK_ADMIN_EMPLOYEE_CODES=shaun.pan
AIRFLOW_LIST=[{"id":"main","alias":"Shaun Airflow","name":"Shaun Airflow","baseUrl":"https://airflow.example.com","username":"...","password":"...","owner":"Shaun"}]
AIRFLOW_ACTIVE_SERVICE_ID=main
```

Deploy the Node/Express app to an environment that can reach Airflow and `https://openapi.seatalk.io`, then configure the SeaTalk WebApp URL to the deployed HTTPS URL.

## Scripts

- `npm run dev`: start local Express server.
- `npm start`: start production-style server.
- `npm test`: run unit/API tests.

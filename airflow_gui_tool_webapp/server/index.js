const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const express = require("express");
const session = require("express-session");
const config = require("../src/config");
const { AirflowClient } = require("../src/airflowClient");
const stateStore = require("../src/stateStore");
const { SeaTalkAuth, isAdminUser, publicUser } = require("./seatalkAuth");
const packageJson = require("../package.json");

function safeFilePart(value) {
  return (
    String(value || "airflow")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 48) || "airflow"
  );
}

function serviceHash(service) {
  return crypto
    .createHash("sha1")
    .update(`${service.baseUrl}|${service.username}`)
    .digest("hex")
    .slice(0, 12);
}

function cookieStorePathForService(appConfig, service) {
  const fileName = `${safeFilePart(service.id)}-${serviceHash(service)}.json`;
  return path.join(appConfig.cookieDir, fileName);
}

function publicAirflowServices(appConfig) {
  return appConfig.airflowServices.map((service) => ({
    id: service.id,
    name: service.name,
    alias: service.alias,
    label: service.label,
    baseUrl: service.baseUrl,
    username: service.username,
    owner: service.owner,
  }));
}

function findAirflowService(appConfig, serviceId) {
  return (
    appConfig.airflowServices.find((service) => service.id === serviceId) ||
    appConfig.airflowServices.find(
      (service) => service.id === appConfig.defaultAirflowServiceId,
    ) ||
    appConfig.airflowServices[0]
  );
}

function asyncHandler(handler) {
  return (req, res, next) => {
    Promise.resolve(handler(req, res, next)).catch(next);
  };
}

function requireAuth(req, res, next) {
  if (!req.session.user) {
    res.status(401).json({
      error: "UNAUTHENTICATED",
      message: "Please open this WebApp in SeaTalk and sign in first.",
    });
    return;
  }
  next();
}

function requireAdmin(appConfig) {
  return (req, res, next) => {
    // if (!isAdminUser(req.session.user, appConfig.seatalk.adminEmployeeCodes)) {
    //   res.status(403).json({
    //     error: "FORBIDDEN",
    //     message: "Only configured SeaTalk admins can perform this action.",
    //   });
    //   return;
    // }
    next();
  };
}

function activeServiceId(appConfig, req) {
  return req.session.airflowServiceId || appConfig.defaultAirflowServiceId;
}

function publicConfig(appConfig, dagConfFieldMapping, req, getClient) {
  const service = findAirflowService(
    appConfig,
    activeServiceId(appConfig, req),
  );
  const client = getClient(service.id);
  const user = publicUser(
    req.session.user,
    appConfig.seatalk.adminEmployeeCodes,
  );
  return {
    ...client.publicConfig,
    services: publicAirflowServices(appConfig),
    dagConfFieldMapping,
    appVersion: packageJson.version,
    update: {
      mode: "web",
      checkOnStart: false,
      checkIntervalHours: 0,
    },
    systemUsername:
      user?.username ||
      (user?.email ? String(user.email).split("@")[0] : "") ||
      user?.name ||
      user?.employeeCode ||
      "",
    canToggleDag: Boolean(user?.isAdmin),
    defaultServiceId: appConfig.defaultAirflowServiceId,
    authUser: user,
    seatalk: {
      configured: Boolean(
        appConfig.seatalk.appId && appConfig.seatalk.appSecret,
      ),
      devAuthAvailable: Boolean(appConfig.auth.allowDevAuth),
    },
  };
}

async function createApp(options = {}) {
  const appConfig = options.config || config;
  if (!appConfig.auth.sessionSecret) {
    throw new Error("SESSION_SECRET is required in production");
  }

  fs.mkdirSync(appConfig.cookieDir, { recursive: true });

  let dagConfFieldMapping =
    options.dagConfFieldMapping || (await appConfig.loadDagConfFieldMapping());
  const seatalkAuth =
    options.seatalkAuth || new SeaTalkAuth(appConfig.seatalk || {});
  const clientCache = new Map();
  const airflowClientFactory =
    options.airflowClientFactory ||
    ((service) =>
      new AirflowClient({
        ...service,
        serviceId: service.id,
        serviceName: service.name,
        cookieStorePath: cookieStorePathForService(appConfig, service),
      }));

  function getClient(serviceId) {
    const service = findAirflowService(appConfig, serviceId);
    if (!clientCache.has(service.id)) {
      clientCache.set(service.id, airflowClientFactory(service));
    }
    return clientCache.get(service.id);
  }

  function getRequestClient(req) {
    return getClient(activeServiceId(appConfig, req));
  }

  const app = express();
  app.disable("x-powered-by");
  app.use(express.json({ limit: "2mb" }));
  app.use(
    session({
      name: "lovito_task_hub.sid",
      secret: appConfig.auth.sessionSecret,
      resave: false,
      saveUninitialized: false,
      cookie: {
        httpOnly: true,
        sameSite: "lax",
        secure: process.env.NODE_ENV === "production",
        maxAge: 7 * 24 * 60 * 60 * 1000,
      },
    }),
  );

  app.get("/api/auth/status", (req, res) => {
    res.json({
      authenticated: Boolean(req.session.user),
      user: publicUser(req.session.user, appConfig.seatalk.adminEmployeeCodes),
      seatalkConfigured: seatalkAuth.isConfigured(),
      devAuthAvailable: Boolean(appConfig.auth.allowDevAuth),
    });
  });

  app.post(
    "/api/auth/dev",
    asyncHandler(async (req, res) => {
      if (!appConfig.auth.allowDevAuth) {
        res.status(403).json({
          error: "DEV_AUTH_DISABLED",
          message: "Development auth is disabled.",
        });
        return;
      }
      req.session.user = {
        ...appConfig.auth.devUser,
        employeeCode: String(appConfig.auth.devUser.employeeCode || "dev"),
      };
      req.session.authMode = "dev";
      res.json({
        user: publicUser(
          req.session.user,
          appConfig.seatalk.adminEmployeeCodes,
        ),
      });
    }),
  );

  app.post(
    "/api/auth/seatalk",
    asyncHandler(async (req, res) => {
      const profile = await seatalkAuth.verifySSOToken(req.body?.ssoToken);
      req.session.user = profile;
      req.session.authMode = "seatalk";
      res.json({
        user: publicUser(profile, appConfig.seatalk.adminEmployeeCodes),
      });
    }),
  );

  app.post("/api/auth/logout", (req, res) => {
    req.session.destroy(() => {
      res.json({ ok: true });
    });
  });

  app.get(
    "/api/init",
    requireAuth,
    asyncHandler(async (req, res) => {
      res.json({
        config: publicConfig(appConfig, dagConfFieldMapping, req, getClient),
        state: stateStore.readState(req.session),
      });
    }),
  );

  app.post(
    "/api/reload-dag-conf-mapping",
    requireAuth,
    asyncHandler(async (_req, res) => {
      dagConfFieldMapping = await appConfig.loadDagConfFieldMapping();
      res.json({ dagConfFieldMapping });
    }),
  );

  app.post(
    "/api/airflow/switch-service",
    requireAuth,
    asyncHandler(async (req, res) => {
      const service = findAirflowService(appConfig, req.body?.serviceId);
      req.session.airflowServiceId = service.id;
      stateStore.savePatch(req.session, { airflowServiceId: service.id });
      res.json({
        config: publicConfig(appConfig, dagConfFieldMapping, req, getClient),
      });
    }),
  );

  app.post(
    "/api/airflow/relogin",
    requireAuth,
    asyncHandler(async (req, res) => {
      res.json(await getRequestClient(req).relogin());
    }),
  );

  app.get(
    "/api/airflow/dags",
    requireAuth,
    asyncHandler(async (req, res) => {
      res.json(await getRequestClient(req).listDags(req.query.owner));
    }),
  );

  app.patch(
    "/api/airflow/dags/:dagId/paused",
    requireAuth,
    requireAdmin(appConfig),
    asyncHandler(async (req, res) => {
      res.json(
        await getRequestClient(req).setDagPaused(
          req.params.dagId,
          req.body?.isPaused,
        ),
      );
    }),
  );

  app.get(
    "/api/airflow/dags/:dagId/code",
    requireAuth,
    asyncHandler(async (req, res) => {
      res.json(await getRequestClient(req).getDagCode(req.params.dagId));
    }),
  );

  app.get(
    "/api/airflow/dags/:dagId/tasks",
    requireAuth,
    asyncHandler(async (req, res) => {
      res.json(await getRequestClient(req).listTasks(req.params.dagId));
    }),
  );

  app.get(
    "/api/airflow/dags/:dagId/dag-runs",
    requireAuth,
    asyncHandler(async (req, res) => {
      res.json(
        await getRequestClient(req).listDagRuns(
          req.params.dagId,
          req.query.limit,
        ),
      );
    }),
  );

  app.post(
    "/api/airflow/dags/:dagId/dag-runs",
    requireAuth,
    requireAdmin(appConfig),
    asyncHandler(async (req, res) => {
      res.json(
        await getRequestClient(req).triggerDag(
          req.params.dagId,
          req.body?.conf || {},
          req.body?.runId || "",
        ),
      );
    }),
  );

  app.get(
    "/api/airflow/dags/:dagId/dag-runs/:dagRunId",
    requireAuth,
    asyncHandler(async (req, res) => {
      res.json(
        await getRequestClient(req).getDagRun(
          req.params.dagId,
          req.params.dagRunId,
        ),
      );
    }),
  );

  app.get(
    "/api/airflow/dags/:dagId/dag-runs/:dagRunId/task-instances",
    requireAuth,
    asyncHandler(async (req, res) => {
      res.json(
        await getRequestClient(req).listTaskInstances(
          req.params.dagId,
          req.params.dagRunId,
        ),
      );
    }),
  );

  app.get(
    "/api/airflow/dags/:dagId/dag-runs/:dagRunId/task-instances/:taskId/tries",
    requireAuth,
    asyncHandler(async (req, res) => {
      res.json(
        await getRequestClient(req).listTaskTries(
          req.params.dagId,
          req.params.dagRunId,
          req.params.taskId,
          req.query.mapIndex,
        ),
      );
    }),
  );

  app.post(
    "/api/airflow/tasks/terminate",
    requireAuth,
    requireAdmin(appConfig),
    asyncHandler(async (req, res) => {
      res.json(await getRequestClient(req).terminateTask(req.body || {}));
    }),
  );

  app.get(
    "/api/airflow/logs",
    requireAuth,
    asyncHandler(async (req, res) => {
      res.json(await getRequestClient(req).getTaskLog(req.query));
    }),
  );

  app.get("/api/state", requireAuth, (req, res) => {
    res.json(stateStore.readState(req.session));
  });

  app.post("/api/state", requireAuth, (req, res) => {
    res.json(stateStore.savePatch(req.session, req.body || {}));
  });

  app.delete("/api/state", requireAuth, (req, res) => {
    res.json(stateStore.clearState(req.session));
  });

  app.post("/api/notify", requireAuth, (_req, res) => {
    res.json({ ok: true });
  });

  app.get("/api/update/check", requireAuth, (_req, res) => {
    res.json({
      enabled: false,
      hasUpdate: false,
      currentVersion: packageJson.version,
      message: "WebApp 版本由部署流程更新，无需桌面自动更新。",
    });
  });

  app.use(express.static(path.join(__dirname, "..", "public")));

  app.use((req, res, next) => {
    if (req.path.startsWith("/api/")) {
      res.status(404).json({ error: "NOT_FOUND", message: "API not found" });
      return;
    }
    next();
  });

  app.use((error, _req, res, _next) => {
    console.error(error);
    res.status(error.status || 500).json({
      error: error.code || "INTERNAL_ERROR",
      message: error.message || "Internal server error",
    });
  });

  return app;
}

if (require.main === module) {
  createApp()
    .then((app) => {
      const { host, port } = config.server;
      app.listen(port, host, () => {
        console.log(
          `Lovito Task Hub WebApp listening at http://${host}:${port}`,
        );
      });
    })
    .catch((error) => {
      console.error(error);
      process.exitCode = 1;
    });
}

module.exports = {
  createApp,
};

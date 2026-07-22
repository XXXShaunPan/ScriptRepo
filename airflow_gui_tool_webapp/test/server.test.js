const assert = require("node:assert/strict");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const request = require("supertest");
const { createApp } = require("../server/index");
const {
  parseEnvContent,
  normalizeAirflowService,
  parseDagConfMappingFromEnv,
} = require("../src/config");

function testConfig({ admins = [], allowDevAuth = true } = {}) {
  const service = normalizeAirflowService({
    id: "main",
    name: "Test Airflow",
    alias: "Test",
    baseUrl: "http://airflow.example.test",
    username: "airflow-user",
    password: "airflow-password",
    owner: "Shaun",
  });
  return {
    app: { title: "Lovito 任务中心" },
    auth: {
      sessionSecret: "test-secret",
      allowDevAuth,
      devUser: {
        employeeCode: "dev",
        email: "dev@example.com",
        name: "Dev User",
      },
    },
    seatalk: {
      appId: "",
      appSecret: "",
      apiBaseUrl: "https://openapi.seatalk.io",
      adminEmployeeCodes: admins,
    },
    airflow: service,
    airflowServices: [service],
    defaultAirflowServiceId: service.id,
    cookieDir: path.join(os.tmpdir(), `airflow-webapp-test-${Date.now()}`),
    loadDagConfFieldMapping: async () => [],
  };
}

function mockAirflowClient(overrides = {}) {
  return {
    publicConfig: {
      serviceId: "main",
      serviceName: "Test Airflow",
      serviceLabel: "Test",
      baseUrl: "http://airflow.example.test",
      username: "airflow-user",
      owner: "Shaun",
    },
    listDags: async (owner) => ({
      owner,
      total: 1,
      totalAirflow: 1,
      dags: [{ dag_id: "demo_dag", owners: ["Shaun"], tags: [] }],
    }),
    triggerDag: async (dagId, conf, runId) => ({
      dag_id: dagId,
      dag_run_id: runId || "manual__test",
      conf,
      state: "queued",
    }),
    ...overrides,
  };
}

test("parses multiline env JSON values", () => {
  const env = parseEnvContent(`A=1\nDAG_CONF_FIELD_MAPPING=[\n{\"dag_name\":\"d\",\"keys\":[\"site\"]}\n]\n`);
  assert.equal(env.A, "1");
  const mapping = parseDagConfMappingFromEnv(env);
  assert.deepEqual(mapping, [
    { dag_name: "d", keys: ["site"], alias: ["site"] },
  ]);
});

test("rejects API init before authentication", async () => {
  const app = await createApp({
    config: testConfig({ allowDevAuth: false }),
    airflowClientFactory: () => mockAirflowClient(),
  });

  const response = await request(app).get("/api/init");
  assert.equal(response.status, 401);
});

test("allows dev-authenticated read-only DAG listing", async () => {
  const app = await createApp({
    config: testConfig(),
    airflowClientFactory: () => mockAirflowClient(),
  });
  const agent = request.agent(app);

  await agent.post("/api/auth/dev").expect(200);
  const response = await agent.get("/api/airflow/dags?owner=Shaun").expect(200);

  assert.equal(response.body.total, 1);
  assert.equal(response.body.dags[0].dag_id, "demo_dag");
});

test("blocks non-admin trigger requests", async () => {
  const app = await createApp({
    config: testConfig({ admins: [] }),
    airflowClientFactory: () => mockAirflowClient(),
  });
  const agent = request.agent(app);

  await agent.post("/api/auth/dev").expect(200);
  await agent
    .post("/api/airflow/dags/demo_dag/dag-runs")
    .send({ conf: {}, runId: "manual__blocked" })
    .expect(403);
});

test("allows admin trigger requests", async () => {
  const app = await createApp({
    config: testConfig({ admins: ["dev"] }),
    airflowClientFactory: () => mockAirflowClient(),
  });
  const agent = request.agent(app);

  await agent.post("/api/auth/dev").expect(200);
  const response = await agent
    .post("/api/airflow/dags/demo_dag/dag-runs")
    .send({ conf: { site: "BR" }, runId: "manual__ok" })
    .expect(200);

  assert.equal(response.body.dag_run_id, "manual__ok");
  assert.deepEqual(response.body.conf, { site: "BR" });
});

test("uses SeaTalk username before numeric employee code", async () => {
  const app = await createApp({
    config: testConfig({ admins: ["shaun.pan"] }),
    seatalkAuth: {
      verifySSOToken: async () => ({
        employeeCode: "123456",
        username: "shaun.pan",
        email: "shaun.pan@example.com",
        name: "Shaun Pan",
      }),
      isConfigured: () => true,
    },
    airflowClientFactory: () => mockAirflowClient(),
  });
  const agent = request.agent(app);

  const authResponse = await agent
    .post("/api/auth/seatalk")
    .send({ ssoToken: "test-token" })
    .expect(200);
  const initResponse = await agent.get("/api/init").expect(200);

  assert.equal(authResponse.body.user.username, "shaun.pan");
  assert.equal(authResponse.body.user.isAdmin, true);
  assert.equal(initResponse.body.config.systemUsername, "shaun.pan");
  assert.equal(initResponse.body.config.canToggleDag, true);
});

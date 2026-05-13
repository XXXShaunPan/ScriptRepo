const fs = require("fs");
const path = require("path");
const { app } = require("electron");

const stateFileName = "airflow-gui-state.json";

function statePath() {
  return path.join(app.getPath("userData"), stateFileName);
}

function readState() {
  try {
    const filePath = statePath();
    if (!fs.existsSync(filePath)) {
      return {};
    }
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return {};
  }
}

function writeState(nextState) {
  const filePath = statePath();
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(nextState, null, 2), "utf8");
  return nextState;
}

function savePatch(patch) {
  const nextState = {
    ...readState(),
    ...patch,
    updatedAt: new Date().toISOString(),
  };
  return writeState(nextState);
}

function clearState() {
  const filePath = statePath();
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }
  return {};
}

module.exports = {
  readState,
  savePatch,
  clearState,
  statePath,
};

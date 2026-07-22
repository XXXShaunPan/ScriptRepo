function readState(session = {}) {
  return session.viewState || {};
}

function savePatch(session = {}, patch = {}) {
  const nextState = {
    ...(session.viewState || {}),
    ...patch,
    updatedAt: new Date().toISOString(),
  };
  session.viewState = nextState;
  return nextState;
}

function clearState(session = {}) {
  session.viewState = {};
  return {};
}

module.exports = {
  readState,
  savePatch,
  clearState,
};

class SeaTalkAuth {
  constructor(seatalkConfig = {}) {
    this.appId = seatalkConfig.appId || "";
    this.appSecret = seatalkConfig.appSecret || "";
    this.apiBaseUrl = String(
      seatalkConfig.apiBaseUrl || "https://openapi.seatalk.io",
    ).replace(/\/+$/, "");
    this.appAccessToken = "";
    this.appAccessTokenExpiresAt = 0;
  }

  isConfigured() {
    return Boolean(this.appId && this.appSecret);
  }

  async postJson(url, body, headers = {}) {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        ...headers,
      },
      body: JSON.stringify(body),
    });
    const text = await response.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = { raw: text };
    }
    return { response, data };
  }

  async getAppAccessToken() {
    if (!this.isConfigured()) {
      throw new Error("SeaTalk SSO is not configured");
    }
    if (this.appAccessToken && Date.now() < this.appAccessTokenExpiresAt) {
      return this.appAccessToken;
    }

    const { response, data } = await this.postJson(
      `${this.apiBaseUrl}/auth/app_access_token`,
      {
        app_id: this.appId,
        app_secret: this.appSecret,
      },
    );

    if (!response.ok || Number(data.code || 0) !== 0 || !data.app_access_token) {
      throw new Error(
        `SeaTalk app access token request failed (${response.status})`,
      );
    }

    this.appAccessToken = data.app_access_token;
    const expireSeconds = Number(data.expire || 0);
    this.appAccessTokenExpiresAt = expireSeconds
      ? Math.max(0, expireSeconds * 1000 - 60 * 1000)
      : Date.now() + 55 * 60 * 1000;
    return this.appAccessToken;
  }

  async verifySSOToken(ssoToken) {
    const token = String(ssoToken || "").trim();
    if (!token) {
      throw new Error("Missing SeaTalk SSO token");
    }

    const appAccessToken = await this.getAppAccessToken();
    const baseVerifyUrl = `${this.apiBaseUrl}/sso/v2/verify`;
    const verifyWithQuery = new URL(baseVerifyUrl);
    verifyWithQuery.searchParams.set("app_access_token", appAccessToken);
    const attempts = [
      {
        url: baseVerifyUrl,
        headers: { Authorization: `Bearer ${appAccessToken}` },
      },
      {
        url: verifyWithQuery.toString(),
        headers: {},
      },
      {
        url: baseVerifyUrl,
        headers: {},
      },
    ];

    const failures = [];
    for (const attempt of attempts) {
      const { response, data } = await this.postJson(
        attempt.url,
        { token },
        attempt.headers,
      );
      if (response.ok && Number(data.code || 0) === 0 && data.profile) {
        logSsoData(data);
        return this.normalizeProfile(data.profile);
      }
      failures.push(data.errMsg || data.message || response.statusText);
    }

    throw new Error(
      `SeaTalk SSO token verification failed: ${failures.filter(Boolean).join("; ") || "unknown error"}`,
    );
  }

  normalizeProfile(profile = {}) {
    const email = normalizeIdentity(profile.email);
    const username = pickFirstIdentity(
      profile.username,
      profile.user_name,
      profile.userName,
      profile.login_name,
      profile.loginName,
      profile.account,
      profile.account_name,
      profile.accountName,
      profile.email && String(profile.email).split("@")[0],
    );
    const name = pickFirstIdentity(
      profile.name,
      profile.display_name,
      profile.displayName,
      profile.real_name,
      profile.realName,
      profile.nick_name,
      profile.nickName,
      profile.nickname,
    );
    return {
      employeeCode: pickFirstIdentity(
        profile.employee_code,
        profile.employeeCode,
        profile.code,
      ),
      username,
      email,
      name,
      raw: profile,
    };
  }
}

function normalizeIdentity(value) {
  return String(value || "").trim();
}

function pickFirstIdentity(...values) {
  for (const value of values) {
    const normalized = normalizeIdentity(value);
    if (normalized) {
      return normalized;
    }
  }
  return "";
}

function identityKey(value) {
  return normalizeIdentity(value).toLowerCase();
}

function userIdentityCandidates(user = {}) {
  const email = normalizeIdentity(user.email);
  return [
    user.employeeCode,
    user.username,
    email,
    email && email.split("@")[0],
    user.name,
  ]
    .map(identityKey)
    .filter(Boolean);
}

function redactForLog(value) {
  if (Array.isArray(value)) {
    return value.map(redactForLog);
  }
  if (!value || typeof value !== "object") {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [
      key,
      /token|secret|password/i.test(key) ? "[redacted]" : redactForLog(item),
    ]),
  );
}

function logSsoData(data) {
  try {
    console.info(
      "[SeaTalk SSO] verify response:",
      JSON.stringify(redactForLog(data), null, 2),
    );
  } catch {
    console.info("[SeaTalk SSO] verify response:", data);
  }
}

function isAdminUser(user, adminEmployeeCodes = []) {
  const allowed = new Set(adminEmployeeCodes.map(identityKey).filter(Boolean));
  return userIdentityCandidates(user).some((candidate) =>
    allowed.has(candidate),
  );
}

function publicUser(user = {}, adminEmployeeCodes = []) {
  if (!user.employeeCode && !user.username && !user.email && !user.name) {
    return null;
  }
  return {
    employeeCode: user.employeeCode || "",
    username: user.username || "",
    email: user.email || "",
    name: user.name || user.username || user.email || user.employeeCode || "",
    isAdmin: isAdminUser(user, adminEmployeeCodes),
  };
}

module.exports = {
  SeaTalkAuth,
  isAdminUser,
  publicUser,
  userIdentityCandidates,
};

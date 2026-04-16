(function(){
  const TABLE_SUFFIX = "hvac";
  const MAX_ROWS_PER_CITY = 1000;
  const STATIC_DATA_URL = "leads-data.json";

  const CITY_TABLES = [
    { slug:"murrieta", label:"Murrieta", table:`murrieta_${TABLE_SUFFIX}`, center:[33.5539,-117.2139], zips:["92562","92563","92564","92595"] },
    { slug:"temecula", label:"Temecula", table:`temecula_${TABLE_SUFFIX}`, center:[33.4936,-117.1484], zips:["92589","92590","92591","92592","92593","92028","92596","92536"] },
    { slug:"menifee", label:"Menifee", table:`menifee_${TABLE_SUFFIX}`, center:[33.6971,-117.1853], zips:["92584","92585","92586","92587","92548","92567"] },
    { slug:"perris", label:"Perris", table:`perris_${TABLE_SUFFIX}`, center:[33.7825,-117.2286], zips:["92570","92571","92572","92599"] },
    { slug:"riverside", label:"Riverside", table:`riverside_${TABLE_SUFFIX}`, center:[33.9806,-117.3755], zips:["92501","92503","92504","92505","92506","92507","92508","92518","92324","92313","91752","92860"] },
    { slug:"oceanside", label:"Oceanside", table:`oceanside_${TABLE_SUFFIX}`, center:[33.1959,-117.3795], zips:["92054","92056","92057","92058","92081","92083","92084","92008","92010","92003"] },
    { slug:"corona", label:"Corona", table:`corona_${TABLE_SUFFIX}`, center:[33.8753,-117.5664], zips:["92877","92878","92879","92880","92881","92882","92883","92870"] },
    { slug:"lake_elsinore", label:"Lake Elsinore", table:`lake_elsinore_${TABLE_SUFFIX}`, center:[33.6681,-117.3273], zips:["92530","92531","92532"] },
    { slug:"moreno_valley", label:"Moreno Valley", table:`moreno_valley_${TABLE_SUFFIX}`, center:[33.9425,-117.2297], zips:["92551","92552","92553","92554","92555","92556","92557","92373","92223"] }
  ];

  function config(){
    return window.SQOUTIQ_SUPABASE || {};
  }

  function isConfigured(){
    return true;
  }

  function hasDirectSupabaseConfig(){
    const cfg = config();
    return Boolean(
      cfg.url &&
      cfg.anonKey &&
      !cfg.url.includes("YOUR_") &&
      !cfg.anonKey.includes("YOUR_")
    );
  }

  function requestHeaders(){
    const cfg = config();
    return {
      apikey: cfg.anonKey,
      Authorization: `Bearer ${cfg.anonKey}`,
      Accept: "application/json"
    };
  }

  function hashText(text){
    let hash = 0;
    for(const ch of String(text || "")){
      hash = ((hash << 5) - hash) + ch.charCodeAt(0);
      hash |= 0;
    }
    return Math.abs(hash);
  }

  function leadScore(row){
    const key = row.SKIPTRACE_WIRELESS_NUMBERS || row.PERSONAL_ADDRESS || row.PERSONAL_ZIP || row.PERSONAL_VERIFIED_EMAIL;
    return 75 + (hashText(key) % 26);
  }

  function signalStrength(score){
    if(score >= 94) return 4;
    if(score >= 86) return 3;
    if(score >= 80) return 2;
    return 1;
  }

  function cleanZip(value){
    return String(value || "").replace(/\D/g, "").slice(0,5);
  }

  function cleanPhone(value){
    const digits = String(value || "").replace(/\D/g, "");
    if(digits.length === 10) return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6)}`;
    if(digits.length === 11 && digits[0] === "1") return `(${digits.slice(1,4)}) ${digits.slice(4,7)}-${digits.slice(7)}`;
    return String(value || "");
  }

  function normalizeLead(row, city){
    const first = String(row.FIRST_NAME || "").trim();
    const last = String(row.LAST_NAME || "").trim();
    const score = leadScore(row);
    return {
      name: `${first} ${last}`.trim() || "Unknown Contact",
      phone: cleanPhone(row.SKIPTRACE_WIRELESS_NUMBERS),
      address: row.PERSONAL_ADDRESS || "",
      city: city.label,
      region: city.slug,
      table: city.table,
      zip: cleanZip(row.PERSONAL_ZIP),
      email: row.PERSONAL_VERIFIED_EMAIL || "",
      score,
      sig: signalStrength(score),
      type: "HVAC",
      timeStamp: row.time_stamp || row.created_at || ""
    };
  }

  async function fetchCityTable(city){
    const cfg = config();
    const base = cfg.url.replace(/\/$/, "");
    const columns = [
      "FIRST_NAME",
      "LAST_NAME",
      "PERSONAL_VERIFIED_EMAIL",
      "SKIPTRACE_WIRELESS_NUMBERS",
      "PERSONAL_ADDRESS",
      "PERSONAL_CITY",
      "PERSONAL_STATE",
      "PERSONAL_ZIP",
      "time_stamp",
      "created_at"
    ].join(",");
    const url = `${base}/rest/v1/${city.table}?select=${columns}&order=created_at.desc&limit=${MAX_ROWS_PER_CITY}`;
    const response = await fetch(url, { headers: requestHeaders() });
    if(!response.ok){
      throw new Error(`${city.table}: ${response.status} ${await response.text()}`);
    }
    const rows = await response.json();
    return rows.map(row => normalizeLead(row, city));
  }

  async function fetchLeads(){
    if(!hasDirectSupabaseConfig()) return fetchStaticLeads();
    const results = await Promise.allSettled(CITY_TABLES.map(fetchCityTable));
    const failures = results.filter(result => result.status === "rejected");
    if(failures.length){
      console.warn("Some city data did not load", failures.map(failure => failure.reason));
    }
    return results.flatMap(result => result.status === "fulfilled" ? result.value : []);
  }

  async function fetchStaticLeads(){
    const response = await fetch(`${STATIC_DATA_URL}?v=${Date.now()}`, { cache: "no-store" });
    if(!response.ok){
      throw new Error(`Static leads data failed: ${response.status} ${await response.text()}`);
    }
    const payload = await response.json();
    return Array.isArray(payload) ? payload : (payload.leads || []);
  }

  function cityOptions(){
    return CITY_TABLES.slice();
  }

  function cityOptionMarkup(){
    return CITY_TABLES.map(city => `<option value="${city.slug}">${city.label}</option>`).join("");
  }

  window.SqoutiqData = {
    CITY_TABLES,
    isConfigured,
    hasDirectSupabaseConfig,
    fetchLeads,
    fetchCityTable,
    leadScore,
    cityOptions,
    cityOptionMarkup
  };
})();

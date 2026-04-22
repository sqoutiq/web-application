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
    { slug:"moreno_valley", label:"Moreno Valley", table:`moreno_valley_${TABLE_SUFFIX}`, center:[33.9425,-117.2297], zips:["92551","92552","92553","92554","92555","92556","92557","92373","92223"] },
    { slug:"opelika", label:"Opelika", table:`opelika_${TABLE_SUFFIX}`, center:[32.6454,-85.3783], zips:["36801","36804","36830"] },
    { slug:"san_antonio", label:"San Antonio", table:`san_antonio_${TABLE_SUFFIX}`, center:[29.617,-98.536], zips:["78249","78258","78260"] }
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

  function parseMoneyValue(value){
    const text = String(value || "").toLowerCase();
    if(!text) return 0;
    const numbers = text.match(/\d+(?:,\d{3})*(?:\.\d+)?/g);
    if(!numbers) return 0;
    const parsed = numbers.map(item => {
      let amount = Number(item.replace(/,/g, ""));
      if(text.includes("k") && amount < 1000) amount *= 1000;
      if(text.includes("m") && amount < 1000000) amount *= 1000000;
      return amount;
    }).filter(Number.isFinite);
    return parsed.length ? Math.max(...parsed) : 0;
  }

  function incomePoints(incomeRange){
    const income = parseMoneyValue(incomeRange);
    if(income >= 250000) return 8;
    if(income >= 200000) return 7;
    if(income >= 150000) return 6;
    if(income >= 100000) return 4;
    if(income >= 75000) return 2;
    return 0;
  }

  function netWorthPoints(netWorth){
    const worth = parseMoneyValue(netWorth);
    if(worth >= 1000000) return 2;
    if(worth >= 500000) return 1;
    return 0;
  }

  function leadScore(row){
    let score = 75;
    if(row.FIRST_NAME && row.LAST_NAME) score += 3;
    if(row.SKIPTRACE_WIRELESS_NUMBERS) score += 4;
    if(row.PERSONAL_VERIFIED_EMAIL) score += 3;
    if(row.PERSONAL_ADDRESS && row.PERSONAL_CITY && row.PERSONAL_STATE && row.PERSONAL_ZIP) score += 5;
    score += incomePoints(row.INCOME_RANGE);
    score += netWorthPoints(row.NET_WORTH);
    return Math.min(100, Math.max(75, score));
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

  function cleanCoordinate(value){
    if(value === null || value === undefined || String(value).trim() === "") return null;
    const number = Number(value);
    if(!Number.isFinite(number)) return null;
    return number >= 24 && number <= 50 ? number : number <= -66 && number >= -125 ? number : null;
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
      lat: cleanCoordinate(row.LATITUDE),
      lng: cleanCoordinate(row.LONGITUDE),
      incomeRange: row.INCOME_RANGE || "",
      netWorth: row.NET_WORTH || "",
      phoneSource: row.PHONE_SOURCE || "",
      phoneDncStatus: row.PHONE_DNC_STATUS || "",
      phoneMatchScore: row.PHONE_MATCH_SCORE || "",
      phoneMatchQuality: row.PHONE_MATCH_QUALITY || "",
      score,
      sig: signalStrength(score),
      type: "HVAC",
      timeStamp: row.time_stamp || row.created_at || ""
    };
  }

  async function fetchCityTable(city){
    const cfg = config();
    const base = cfg.url.replace(/\/$/, "");
    const coreColumns = [
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
    ];
    const enrichColumns = [
      "NET_WORTH",
      "INCOME_RANGE"
    ];
    const geoColumns = ["LATITUDE", "LONGITUDE"];
    const phoneQualityColumns = [
      "PHONE_SOURCE",
      "PHONE_DNC_STATUS",
      "PHONE_MATCH_SCORE",
      "PHONE_MATCH_QUALITY"
    ];
    const columns = coreColumns.concat(enrichColumns, geoColumns, phoneQualityColumns).join(",");
    const url = `${base}/rest/v1/${city.table}?select=${columns}&order=created_at.desc&limit=${MAX_ROWS_PER_CITY}`;
    let response = await fetch(url, { headers: requestHeaders() });
    if(!response.ok && response.status === 400){
      const fallbackUrl = `${base}/rest/v1/${city.table}?select=${coreColumns.join(",")}&order=created_at.desc&limit=${MAX_ROWS_PER_CITY}`;
      response = await fetch(fallbackUrl, { headers: requestHeaders() });
    }
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

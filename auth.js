(function(){
  const SESSION_KEY = "sqoutiq-auth-session";

  function config(){
    return window.SQOUTIQ_SUPABASE || {};
  }

  function isConfigured(){
    const cfg = config();
    return Boolean(cfg.url && cfg.anonKey && !cfg.url.includes("YOUR_") && !cfg.anonKey.includes("YOUR_"));
  }

  function getSession(){
    try{return JSON.parse(localStorage.getItem(SESSION_KEY) || "null")}catch(e){return null}
  }

  function setSession(session){
    localStorage.setItem(SESSION_KEY, JSON.stringify(session));
  }

  function clearSession(){
    localStorage.removeItem(SESSION_KEY);
  }

  function isExpired(session){
    if(!session?.expires_at) return false;
    return Date.now() >= Number(session.expires_at) * 1000;
  }

  function isLoggedIn(){
    const session=getSession();
    return Boolean(session?.access_token && !isExpired(session));
  }

  function currentUser(){
    return getSession()?.user || null;
  }

  function isAdmin(){
    const user=currentUser();
    const appRole=user?.app_metadata?.role || user?.app_metadata?.user_role;
    const metaRole=user?.user_metadata?.role || user?.user_metadata?.user_role;
    return appRole === "admin" || metaRole === "admin";
  }

  async function login(email,password){
    if(!isConfigured()) throw new Error("Login is not configured yet. Contact your workspace admin.");
    const cfg=config();
    const base=cfg.url.replace(/\/$/,"");
    const response=await fetch(`${base}/auth/v1/token?grant_type=password`,{
      method:"POST",
      headers:{
        apikey:cfg.anonKey,
        "Content-Type":"application/json"
      },
      body:JSON.stringify({email,password})
    });
    const payload=await response.json().catch(()=>({}));
    if(!response.ok){
      const detail = payload.error_description || payload.msg || payload.error || `HTTP ${response.status}`;
      throw new Error(`Login failed: ${detail}`);
    }
    if(payload.user && !payload.user.user_metadata?.full_name){
      const meta=payload.user.user_metadata || {};
      const first=meta.first_name || meta.firstName || meta.given_name || meta.givenName || "";
      const last=meta.last_name || meta.lastName || meta.family_name || meta.familyName || "";
      payload.user.user_metadata = {
        ...meta,
        full_name: `${first} ${last}`.trim() || (payload.user.email ? payload.user.email.split("@")[0].replace(/[._-]+/g," ").replace(/\b\w/g,ch=>ch.toUpperCase()) : "Viewer")
      };
    }
    setSession(payload);
    return payload;
  }

  function logout(){
    clearSession();
    window.location.href="/";
  }

  function requireAuth(){
    if(!isLoggedIn()){
      window.location.replace("/");
      return false;
    }
    return true;
  }

  function requireAdmin(){
    if(!requireAuth()) return false;
    if(!isAdmin()){
      window.location.replace("/dash");
      return false;
    }
    return true;
  }

  function redirectIfLoggedIn(){
    if(isLoggedIn()) window.location.replace("/dash");
  }

  function bindLogout(){
    document.querySelectorAll("[data-logout]").forEach(el=>el.addEventListener("click",logout));
  }

  window.SqoutiqAuth = {SESSION_KEY,getSession,currentUser,isLoggedIn,isAdmin,login,logout,requireAuth,requireAdmin,redirectIfLoggedIn,bindLogout};
  if(document.readyState === "loading") document.addEventListener("DOMContentLoaded",bindLogout);
  else bindLogout();
})();

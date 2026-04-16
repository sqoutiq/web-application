(function(){
  function parseJson(value){
    try{return JSON.parse(value)}catch(e){return null}
  }

  function findSupabaseUser(){
    const stores=[localStorage,sessionStorage];
    for(const store of stores){
      for(let i=0;i<store.length;i++){
        const key=store.key(i) || "";
        if(!key.includes("auth-token") && !key.includes("supabase.auth")) continue;
        const payload=parseJson(store.getItem(key));
        const user=payload?.user || payload?.currentSession?.user || payload?.session?.user;
        if(user) return user;
      }
    }
    return null;
  }

  function displayName(user){
    const meta=user?.user_metadata || {};
    return meta.full_name || meta.name || meta.display_name || user?.email?.split("@")[0] || "Viewer";
  }

  function initials(name,email){
    const source=(name && name !== "Viewer" ? name : email || "Viewer").trim();
    const parts=source.includes("@") ? [source[0]] : source.split(/\s+/).filter(Boolean);
    return parts.slice(0,2).map(part=>part[0]).join("").toUpperCase() || "V";
  }

  function applyProfile(){
    const user=findSupabaseUser();
    const name=displayName(user);
    const email=user?.email || "";
    const avatar=initials(name,email);

    document.querySelectorAll(".profile-name,[data-profile-name]").forEach(el=>{el.textContent=name});
    document.querySelectorAll(".profile-avatar,.profile-av-lg,[data-profile-avatar]").forEach(el=>{el.textContent=avatar});
    document.querySelectorAll("[data-profile-email]").forEach(el=>{el.textContent=email || "Signed in viewer"});
    document.querySelectorAll("input[data-profile-name-input]").forEach(el=>{el.value=name});
    document.querySelectorAll("input[data-profile-email-input]").forEach(el=>{el.value=email});
  }

  if(document.readyState === "loading") document.addEventListener("DOMContentLoaded",applyProfile);
  else applyProfile();
})();

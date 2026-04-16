import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const citySet = new Set([
  "murrieta",
  "temecula",
  "menifee",
  "perris",
  "riverside",
  "oceanside",
  "corona",
  "lake_elsinore",
  "moreno_valley",
]);

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return json({ error: "Method not allowed." }, 405);

  const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
  const serviceKey = Deno.env.get("SERVICE_ROLE_KEY") || "";
  if (!supabaseUrl || !serviceKey) return json({ error: "Server is not configured." }, 500);

  const token = (req.headers.get("Authorization") || "").replace(/^Bearer\s+/i, "");
  if (!token) return json({ error: "Unauthorized." }, 401);

  const admin = createClient(supabaseUrl, serviceKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  const { data: callerData, error: callerError } = await admin.auth.getUser(token);
  if (callerError || !callerData.user) return json({ error: "Unauthorized." }, 401);

  const caller = callerData.user;
  const callerRole = caller.app_metadata?.role || caller.user_metadata?.role;
  if (callerRole !== "admin") return json({ error: "Admin access required." }, 403);

  const payload = await req.json().catch(() => null);
  if (!payload) return json({ error: "Invalid request body." }, 400);

  const fullName = String(payload.full_name || "").trim();
  const email = String(payload.email || "").trim().toLowerCase();
  const phone = String(payload.phone || "").trim();
  const password = String(payload.password || "");
  const role = payload.role === "admin" ? "admin" : "viewer";
  const metros = Array.isArray(payload.metros) ? payload.metros.filter((metro: string) => citySet.has(metro)) : [];

  if (!fullName || !email || !password) return json({ error: "Name, email, and password are required." }, 400);
  if (password.length < 8) return json({ error: "Password must be at least 8 characters." }, 400);
  if (!metros.length) return json({ error: "Select at least one metro." }, 400);

  const { data: created, error: createError } = await admin.auth.admin.createUser({
    email,
    password,
    email_confirm: true,
    user_metadata: { full_name: fullName, phone, metros, role },
    app_metadata: { role, metros },
  });

  if (createError) return json({ error: createError.message }, 400);

  return json({
    id: created.user?.id,
    email: created.user?.email,
    role,
    metros,
  });
});

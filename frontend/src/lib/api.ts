export type Lead = {
  id: number;
  company_name: string;
  primary_title: string;
  lead_type: string;
  freshness_label: string;
  qualification_fit_label: string;
  confidence_label: string;
  current_status?: string | null;
  source_platform?: string | null;
  source_url?: string | null;
  explanation?: string | null;
  surfaced_at?: string | null;
};

export type CandidateProfile = {
  target_titles: string[];
  target_locations: string[];
  preferred_domains: string[];
  focus_keywords: string[];
  excluded_keywords: string[];
  notes?: string | null;
};

type LeadsResponse = {
  items: Lead[];
};

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api";

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });
  if (!response.ok) {
    throw new Error(`Request failed for ${path}: ${response.status}`);
  }
  return (await response.json()) as T;
}

export async function getLeads(params: Record<string, string | boolean | number | undefined> = {}): Promise<Lead[]> {
  const searchParams = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined) {
      return;
    }
    searchParams.set(key, String(value));
  });
  const query = searchParams.toString();
  const payload = await requestJson<LeadsResponse>(`/opportunities${query ? `?${query}` : ""}`);
  return payload.items;
}

export function getCandidateProfile(): Promise<CandidateProfile> {
  return requestJson<CandidateProfile>("/candidate-profile");
}

export function setApplicationStatus(payload: {
  lead_id: number;
  current_status: string;
  notes?: string;
  date_applied?: string;
}): Promise<{ status: string; lead_id: number; current_status: string }> {
  return requestJson("/applications/status", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

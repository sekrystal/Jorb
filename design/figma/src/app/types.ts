export interface Job {
  id: string;
  title: string;
  company: string;
  location: string;
  workMode: 'remote' | 'hybrid' | 'onsite';
  description: string;
  fullDescription: string;
  matchScore: number;
  matchLabel: 'Strong Match' | 'Medium Match' | 'Stretch';
  recommendationExplanation: string;
  tags: string[];
  postedDate: string;
  salary?: string;
  source?: string;
  state: 'new' | 'saved' | 'applied' | 'dismissed';
  // Expanded explanation fields
  whyThisJob?: string;
  whatYouAreMissing?: string;
  suggestedNextSteps?: string;
}

export interface AppliedJobStatus {
  jobId: string;
  status: 'applied' | 'interviewing' | 'rejected';
  notes?: string;
  appliedDate: string;
}

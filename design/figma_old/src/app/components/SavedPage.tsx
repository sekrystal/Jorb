import { useState } from "react";
import { Job } from "../types";
import { mockJobs } from "../data/mockJobs";
import { JobCard } from "./JobCard";
import { JobDetailPanel } from "./JobDetailPanel";
import { Bookmark } from "lucide-react";

export function SavedPage() {
  const [jobs, setJobs] = useState<Job[]>(mockJobs);
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);

  const savedJobs = jobs.filter(job => job.state === "saved");

  const handleApply = (jobId: string) => {
    setJobs(jobs.map(job => 
      job.id === jobId ? { ...job, state: "applied" as const } : job
    ));
    if (selectedJob?.id === jobId) {
      setSelectedJob({ ...selectedJob, state: "applied" });
    }
  };

  const handleUnsave = (jobId: string) => {
    setJobs(jobs.map(job => 
      job.id === jobId ? { ...job, state: "new" as const } : job
    ));
    if (selectedJob?.id === jobId) {
      setSelectedJob({ ...selectedJob, state: "new" });
    }
  };

  const handleDismiss = (jobId: string) => {
    setJobs(jobs.map(job => 
      job.id === jobId ? { ...job, state: "dismissed" as const } : job
    ));
    if (selectedJob?.id === jobId) {
      setSelectedJob({ ...selectedJob, state: "dismissed" });
    }
  };

  return (
    <div className="h-screen flex">
      <div className={`flex-1 flex flex-col ${selectedJob ? "mr-[600px]" : ""}`}>
        <div className="bg-white border-b border-gray-200 p-6">
          <div className="flex items-center gap-2">
            <Bookmark className="w-5 h-5 text-gray-600" />
            <h1 className="font-semibold text-xl text-gray-900">Saved Jobs</h1>
            <span className="text-sm text-gray-500">({savedJobs.length})</span>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto p-6">
          {savedJobs.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-96">
              <Bookmark className="w-12 h-12 text-gray-300 mb-4" />
              <h3 className="font-semibold text-lg text-gray-900 mb-2">
                No saved jobs yet
              </h3>
              <p className="text-sm text-gray-600">
                Save jobs you're interested in to review them later.
              </p>
            </div>
          ) : (
            <div className="space-y-4 max-w-4xl">
              {savedJobs.map((job) => (
                <JobCard
                  key={job.id}
                  job={job}
                  onClick={() => setSelectedJob(job)}
                  onSave={() => handleUnsave(job.id)}
                  onApply={() => handleApply(job.id)}
                  onDismiss={() => handleDismiss(job.id)}
                />
              ))}
            </div>
          )}
        </div>
      </div>

      {selectedJob && (
        <JobDetailPanel
          job={selectedJob}
          onClose={() => setSelectedJob(null)}
          onSave={() => handleUnsave(selectedJob.id)}
          onApply={() => handleApply(selectedJob.id)}
          onDismiss={() => handleDismiss(selectedJob.id)}
        />
      )}
    </div>
  );
}

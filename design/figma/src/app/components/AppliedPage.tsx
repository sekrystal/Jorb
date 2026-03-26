import { useState } from "react";
import { Job, AppliedJobStatus } from "../types";
import { mockJobs } from "../data/mockJobs";
import { JobDetailPanel } from "./JobDetailPanel";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { Textarea } from "./ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "./ui/select";
import { CheckCircle, MapPin, Calendar } from "lucide-react";

export function AppliedPage() {
  const [jobs] = useState<Job[]>(mockJobs);
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);
  const [appliedStatuses, setAppliedStatuses] = useState<Record<string, AppliedJobStatus>>({});

  const appliedJobs = jobs.filter(job => job.state === "applied");

  const getStatusColor = (status: string) => {
    switch (status) {
      case "applied":
        return "bg-blue-100 text-blue-800";
      case "interviewing":
        return "bg-purple-100 text-purple-800";
      case "rejected":
        return "bg-gray-100 text-gray-800";
      default:
        return "bg-gray-100 text-gray-800";
    }
  };

  const updateStatus = (jobId: string, status: AppliedJobStatus["status"]) => {
    setAppliedStatuses({
      ...appliedStatuses,
      [jobId]: {
        ...appliedStatuses[jobId],
        jobId,
        status,
        appliedDate: appliedStatuses[jobId]?.appliedDate || new Date().toISOString(),
      },
    });
  };

  const updateNotes = (jobId: string, notes: string) => {
    setAppliedStatuses({
      ...appliedStatuses,
      [jobId]: {
        ...appliedStatuses[jobId],
        jobId,
        status: appliedStatuses[jobId]?.status || "applied",
        appliedDate: appliedStatuses[jobId]?.appliedDate || new Date().toISOString(),
        notes,
      },
    });
  };

  return (
    <div className="h-screen flex">
      <div className={`flex-1 flex flex-col ${selectedJob ? "mr-[600px]" : ""}`}>
        <div className="bg-white border-b border-gray-200 p-6">
          <div className="flex items-center gap-2">
            <CheckCircle className="w-5 h-5 text-gray-600" />
            <h1 className="font-semibold text-xl text-gray-900">Applied Jobs</h1>
            <span className="text-sm text-gray-500">({appliedJobs.length})</span>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto p-6">
          {appliedJobs.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-96">
              <CheckCircle className="w-12 h-12 text-gray-300 mb-4" />
              <h3 className="font-semibold text-lg text-gray-900 mb-2">
                No applications yet
              </h3>
              <p className="text-sm text-gray-600">
                Apply to jobs to track your applications here.
              </p>
            </div>
          ) : (
            <div className="space-y-4 max-w-4xl">
              {appliedJobs.map((job) => {
                const status = appliedStatuses[job.id];
                return (
                  <div
                    key={job.id}
                    className="bg-white border border-gray-200 rounded-lg p-5 hover:shadow-md transition-shadow"
                  >
                    <div className="flex items-start gap-4 mb-4">
                      <div className="flex-1">
                        <div className="flex items-center gap-2 mb-2">
                          <h3
                            className="font-semibold text-lg text-gray-900 cursor-pointer hover:text-blue-600"
                            onClick={() => setSelectedJob(job)}
                          >
                            {job.title}
                          </h3>
                          <Badge className={getStatusColor(status?.status || "applied")}>
                            {status?.status || "applied"}
                          </Badge>
                        </div>
                        <div className="flex items-center gap-2 text-sm text-gray-600 mb-3">
                          <span className="font-medium">{job.company}</span>
                          <span>•</span>
                          <div className="flex items-center gap-1">
                            <MapPin className="w-3 h-3" />
                            <span>{job.location}</span>
                          </div>
                          <span>•</span>
                          <div className="flex items-center gap-1">
                            <Calendar className="w-3 h-3" />
                            <span>Applied 3 days ago</span>
                          </div>
                        </div>

                        <div className="flex items-center gap-3 mb-4">
                          <Select
                            value={status?.status || "applied"}
                            onValueChange={(value: AppliedJobStatus["status"]) =>
                              updateStatus(job.id, value)
                            }
                          >
                            <SelectTrigger className="w-40">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="applied">Applied</SelectItem>
                              <SelectItem value="interviewing">Interviewing</SelectItem>
                              <SelectItem value="rejected">Rejected</SelectItem>
                            </SelectContent>
                          </Select>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setSelectedJob(job)}
                          >
                            View Details
                          </Button>
                        </div>

                        <div>
                          <label className="text-xs font-medium text-gray-500 mb-1 block">
                            Notes
                          </label>
                          <Textarea
                            placeholder="Add notes about this application..."
                            value={status?.notes || ""}
                            onChange={(e) => updateNotes(job.id, e.target.value)}
                            className="text-sm min-h-20"
                          />
                        </div>
                      </div>
                    </div>

                    {/* Placeholder for future features */}
                    <div className="border-t border-gray-100 pt-4 mt-4">
                      <div className="text-xs text-gray-400 italic">
                        Status progression timeline and follow-up reminders coming soon
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {selectedJob && (
        <JobDetailPanel
          job={selectedJob}
          onClose={() => setSelectedJob(null)}
        />
      )}
    </div>
  );
}

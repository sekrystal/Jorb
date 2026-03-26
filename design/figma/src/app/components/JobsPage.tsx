import { useState } from "react";
import { Job } from "../types";
import { mockJobs } from "../data/mockJobs";
import { JobCard } from "./JobCard";
import { JobDetailPanel } from "./JobDetailPanel";
import { DismissDialog } from "./DismissDialog";
import { Input } from "./ui/input";
import { Button } from "./ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "./ui/select";
import { Switch } from "./ui/switch";
import { Label } from "./ui/label";
import { Search, RefreshCw, MapPin } from "lucide-react";
import { Skeleton } from "./ui/skeleton";

export function JobsPage() {
  const [jobs, setJobs] = useState<Job[]>(mockJobs);
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [locationFilter, setLocationFilter] = useState("");
  const [remoteOnly, setRemoteOnly] = useState(false);
  const [sortBy, setSortBy] = useState("match");
  const [isLoading, setIsLoading] = useState(false);
  const [lastUpdated] = useState("12 minutes ago");
  const [showDismissed, setShowDismissed] = useState(false);
  const [dismissDialogOpen, setDismissDialogOpen] = useState(false);
  const [jobToDismiss, setJobToDismiss] = useState<Job | null>(null);
  const [dismissReasons, setDismissReasons] = useState<Record<string, string[]>>({});

  const handleRefresh = () => {
    setIsLoading(true);
    setTimeout(() => {
      setIsLoading(false);
    }, 1500);
  };

  const handleSave = (jobId: string) => {
    setJobs(jobs.map(job => 
      job.id === jobId ? { ...job, state: "saved" as const } : job
    ));
    if (selectedJob?.id === jobId) {
      setSelectedJob({ ...selectedJob, state: "saved" });
    }
  };

  const handleApply = (jobId: string) => {
    setJobs(jobs.map(job => 
      job.id === jobId ? { ...job, state: "applied" as const } : job
    ));
    if (selectedJob?.id === jobId) {
      setSelectedJob({ ...selectedJob, state: "applied" });
    }
  };

  const handleDismiss = (jobId: string) => {
    const job = jobs.find(j => j.id === jobId);
    if (job) {
      setJobToDismiss(job);
      setDismissDialogOpen(true);
    }
  };

  const handleConfirmDismiss = (reasons: string[]) => {
    if (jobToDismiss) {
      setDismissReasons({
        ...dismissReasons,
        [jobToDismiss.id]: reasons
      });
      setJobs(jobs.map(job => 
        job.id === jobToDismiss.id ? { ...job, state: "dismissed" as const } : job
      ));
      if (selectedJob?.id === jobToDismiss.id) {
        setSelectedJob({ ...selectedJob, state: "dismissed" });
      }
    }
    setDismissDialogOpen(false);
    setJobToDismiss(null);
  };

  const handleRecoverJob = (jobId: string) => {
    setJobs(jobs.map(job => 
      job.id === jobId ? { ...job, state: "new" as const } : job
    ));
    if (selectedJob?.id === jobId) {
      setSelectedJob({ ...selectedJob, state: "new" });
    }
  };

  // Filter and sort jobs
  let filteredJobs = jobs.filter(job => showDismissed || job.state !== "dismissed");

  if (searchQuery) {
    filteredJobs = filteredJobs.filter(job =>
      job.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
      job.company.toLowerCase().includes(searchQuery.toLowerCase())
    );
  }

  if (locationFilter) {
    filteredJobs = filteredJobs.filter(job =>
      job.location.toLowerCase().includes(locationFilter.toLowerCase())
    );
  }

  if (remoteOnly) {
    filteredJobs = filteredJobs.filter(job => job.workMode === "remote");
  }

  if (sortBy === "match") {
    filteredJobs = [...filteredJobs].sort((a, b) => b.matchScore - a.matchScore);
  } else if (sortBy === "newest") {
    filteredJobs = [...filteredJobs].sort((a, b) => {
      const getDays = (dateStr: string) => {
        const match = dateStr.match(/(\d+)/);
        return match ? parseInt(match[0]) : 999;
      };
      return getDays(a.postedDate) - getDays(b.postedDate);
    });
  }

  return (
    <div className="h-screen flex">
      <div className={`flex-1 flex flex-col ${selectedJob ? "mr-[600px]" : ""}`}>
        {/* Top Bar */}
        <div className="bg-white border-b border-gray-200 p-4">
          <div className="flex items-center gap-4 mb-4">
            <div className="relative flex-1 max-w-md">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input
                placeholder="Search roles or keywords..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-9"
              />
            </div>

            <div className="relative w-48">
              <MapPin className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400 z-10" />
              <Input
                placeholder="Location"
                value={locationFilter}
                onChange={(e) => setLocationFilter(e.target.value)}
                className="pl-9"
              />
            </div>

            <div className="flex items-center gap-2">
              <Switch
                id="remote-only"
                checked={remoteOnly}
                onCheckedChange={setRemoteOnly}
              />
              <Label htmlFor="remote-only" className="text-sm cursor-pointer">
                Remote only
              </Label>
            </div>

            <div className="flex items-center gap-2">
              <Switch
                id="show-dismissed"
                checked={showDismissed}
                onCheckedChange={setShowDismissed}
              />
              <Label htmlFor="show-dismissed" className="text-sm cursor-pointer">
                Show dismissed
              </Label>
            </div>

            <Select value={sortBy} onValueChange={setSortBy}>
              <SelectTrigger className="w-40">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="match">Best Match</SelectItem>
                <SelectItem value="newest">Newest</SelectItem>
              </SelectContent>
            </Select>

            <div className="ml-auto flex items-center gap-3">
              <span className="text-xs text-gray-500">
                Last updated: {lastUpdated}
              </span>
              <Button
                variant="outline"
                size="sm"
                onClick={handleRefresh}
                disabled={isLoading}
                className="gap-2"
              >
                <RefreshCw className={`w-4 h-4 ${isLoading ? "animate-spin" : ""}`} />
                Refresh Jobs
              </Button>
            </div>
          </div>
        </div>

        {/* Job List */}
        <div className="flex-1 overflow-y-auto p-6">
          {isLoading ? (
            <div className="space-y-4">
              {[1, 2, 3].map((i) => (
                <div key={i} className="bg-white border border-gray-200 rounded-lg p-5 space-y-3">
                  <div className="flex justify-between">
                    <div className="flex-1 space-y-2">
                      <Skeleton className="h-6 w-2/3" />
                      <Skeleton className="h-4 w-1/2" />
                    </div>
                    <Skeleton className="h-14 w-14 rounded-lg" />
                  </div>
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-3/4" />
                  <div className="flex gap-2">
                    <Skeleton className="h-6 w-20" />
                    <Skeleton className="h-6 w-20" />
                    <Skeleton className="h-6 w-20" />
                  </div>
                </div>
              ))}
            </div>
          ) : filteredJobs.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-96">
              <div className="text-center max-w-md">
                <h3 className="font-semibold text-lg text-gray-900 mb-2">
                  No matching jobs found
                </h3>
                <p className="text-sm text-gray-600 mb-4">
                  Try adjusting your filters or search criteria to see more results.
                </p>
                <Button
                  variant="outline"
                  onClick={() => {
                    setSearchQuery("");
                    setLocationFilter("");
                    setRemoteOnly(false);
                  }}
                >
                  Clear Filters
                </Button>
              </div>
            </div>
          ) : (
            <div className="space-y-4 max-w-4xl">
              {filteredJobs.map((job) => (
                <JobCard
                  key={job.id}
                  job={job}
                  onClick={() => setSelectedJob(job)}
                  onSave={() => handleSave(job.id)}
                  onApply={() => handleApply(job.id)}
                  onDismiss={() => handleDismiss(job.id)}
                  onRecover={job.state === "dismissed" ? () => handleRecoverJob(job.id) : undefined}
                />
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Detail Panel */}
      {selectedJob && (
        <JobDetailPanel
          job={selectedJob}
          onClose={() => setSelectedJob(null)}
          onSave={() => handleSave(selectedJob.id)}
          onApply={() => handleApply(selectedJob.id)}
          onDismiss={() => handleDismiss(selectedJob.id)}
        />
      )}

      {/* Dismiss Dialog */}
      <DismissDialog
        open={dismissDialogOpen}
        onOpenChange={setDismissDialogOpen}
        onConfirm={handleConfirmDismiss}
        jobTitle={jobToDismiss?.title || ""}
      />
    </div>
  );
}
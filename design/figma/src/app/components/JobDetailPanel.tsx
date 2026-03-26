import { Job } from "../types";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { X, Bookmark, CheckCircle, MapPin, DollarSign, Calendar } from "lucide-react";

interface JobDetailPanelProps {
  job: Job;
  onClose: () => void;
  onSave?: () => void;
  onApply?: () => void;
  onDismiss?: () => void;
}

export function JobDetailPanel({ job, onClose, onSave, onApply, onDismiss }: JobDetailPanelProps) {
  const getMatchColor = (score: number) => {
    if (score >= 85) return "bg-green-100 text-green-800 border-green-200";
    if (score >= 70) return "bg-yellow-100 text-yellow-800 border-yellow-200";
    return "bg-orange-100 text-orange-800 border-orange-200";
  };

  return (
    <div className="w-[600px] h-screen bg-white border-l border-gray-200 fixed right-0 top-0 overflow-y-auto">
      <div className="sticky top-0 bg-white border-b border-gray-200 p-4 flex items-start justify-between z-10">
        <div className="flex-1">
          <h2 className="font-semibold text-xl text-gray-900 mb-1">{job.title}</h2>
          <div className="text-sm text-gray-600">
            <span className="font-medium">{job.company}</span> • {job.location}
          </div>
        </div>
        <button
          onClick={onClose}
          className="p-2 hover:bg-gray-100 rounded-md transition-colors"
          aria-label="Close"
        >
          <X className="w-5 h-5" />
        </button>
      </div>

      <div className="p-6 space-y-6">
        {/* Match Score Section */}
        <div className="flex items-center gap-4">
          <div
            className={`flex items-center justify-center w-16 h-16 rounded-lg border font-semibold text-xl ${getMatchColor(
              job.matchScore
            )}`}
          >
            {job.matchScore}
          </div>
          <div>
            <div className="font-medium text-gray-900">{job.matchLabel}</div>
            <div className="text-sm text-gray-600">{job.recommendationExplanation}</div>
          </div>
        </div>

        {/* Metadata */}
        <div className="grid grid-cols-2 gap-4 p-4 bg-gray-50 rounded-lg">
          <div className="flex items-start gap-2">
            <MapPin className="w-4 h-4 text-gray-500 mt-0.5" />
            <div>
              <div className="text-xs text-gray-500">Location & Mode</div>
              <div className="text-sm font-medium">{job.location}</div>
              <Badge variant="outline" className="mt-1 text-xs">
                {job.workMode}
              </Badge>
            </div>
          </div>
          {job.salary && (
            <div className="flex items-start gap-2">
              <DollarSign className="w-4 h-4 text-gray-500 mt-0.5" />
              <div>
                <div className="text-xs text-gray-500">Salary Range</div>
                <div className="text-sm font-medium">{job.salary}</div>
              </div>
            </div>
          )}
          <div className="flex items-start gap-2">
            <Calendar className="w-4 h-4 text-gray-500 mt-0.5" />
            <div>
              <div className="text-xs text-gray-500">Posted</div>
              <div className="text-sm font-medium">{job.postedDate}</div>
            </div>
          </div>
          {job.source && (
            <div className="flex items-start gap-2">
              <div className="w-4 h-4 mt-0.5" />
              <div>
                <div className="text-xs text-gray-500">Source</div>
                <div className="text-sm font-medium">{job.source}</div>
              </div>
            </div>
          )}
        </div>

        {/* Tags */}
        <div>
          <div className="text-xs font-medium text-gray-500 mb-2">KEY SIGNALS</div>
          <div className="flex flex-wrap gap-2">
            {job.tags.map((tag) => (
              <Badge key={tag} variant="secondary">
                {tag}
              </Badge>
            ))}
          </div>
        </div>

        {/* Actions */}
        <div className="flex gap-2">
          <Button
            className="flex-1"
            onClick={onApply}
            disabled={job.state === "applied"}
          >
            <CheckCircle className="w-4 h-4 mr-2" />
            {job.state === "applied" ? "Applied" : "Apply Now"}
          </Button>
          <Button
            variant="outline"
            onClick={onSave}
            disabled={job.state === "saved"}
          >
            <Bookmark className="w-4 h-4 mr-2" />
            {job.state === "saved" ? "Saved" : "Save"}
          </Button>
          <Button variant="ghost" onClick={onDismiss} disabled={job.state === "dismissed"}>
            <X className="w-4 h-4" />
          </Button>
        </div>

        {/* Expanded Explanation */}
        <div className="space-y-4">
          {job.whyThisJob && (
            <div className="bg-blue-50 border border-blue-100 rounded-lg p-4">
              <div className="font-medium text-blue-900 mb-1">Why this job</div>
              <p className="text-sm text-blue-800">{job.whyThisJob}</p>
            </div>
          )}

          {job.whatYouAreMissing && (
            <div className="bg-amber-50 border border-amber-100 rounded-lg p-4">
              <div className="font-medium text-amber-900 mb-1">What you're missing</div>
              <p className="text-sm text-amber-800">{job.whatYouAreMissing}</p>
            </div>
          )}

          {job.suggestedNextSteps && (
            <div className="bg-green-50 border border-green-100 rounded-lg p-4">
              <div className="font-medium text-green-900 mb-1">Suggested next steps</div>
              <p className="text-sm text-green-800">{job.suggestedNextSteps}</p>
            </div>
          )}
        </div>

        {/* Full Description */}
        <div>
          <div className="text-xs font-medium text-gray-500 mb-2">FULL DESCRIPTION</div>
          <div className="prose prose-sm max-w-none">
            {job.fullDescription.split('\n').map((paragraph, idx) => (
              <p key={idx} className="text-sm text-gray-700 mb-3">
                {paragraph}
              </p>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

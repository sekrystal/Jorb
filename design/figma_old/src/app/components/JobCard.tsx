import { Job } from "../types";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { Bookmark, CheckCircle, X, MapPin, ThumbsUp, ThumbsDown, RotateCcw } from "lucide-react";

interface JobCardProps {
  job: Job;
  onClick: () => void;
  onSave?: () => void;
  onApply?: () => void;
  onDismiss?: () => void;
  onRecover?: () => void;
}

export function JobCard({ job, onClick, onSave, onApply, onDismiss, onRecover }: JobCardProps) {
  const getMatchColor = (score: number) => {
    if (score >= 85) return "bg-green-100 text-green-800 border-green-200";
    if (score >= 70) return "bg-yellow-100 text-yellow-800 border-yellow-200";
    return "bg-orange-100 text-orange-800 border-orange-200";
  };

  const getWorkModeColor = (mode: string) => {
    if (mode === "remote") return "bg-blue-100 text-blue-800";
    if (mode === "hybrid") return "bg-purple-100 text-purple-800";
    return "bg-gray-100 text-gray-800";
  };

  return (
    <div
      className={`bg-white border rounded-lg p-5 hover:shadow-md transition-shadow cursor-pointer ${
        job.state === "dismissed" ? "opacity-50" : ""
      } ${job.state === "saved" ? "border-blue-300" : "border-gray-200"}`}
      onClick={onClick}
    >
      {/* State indicator */}
      {job.state === "saved" && (
        <div className="flex items-center gap-1 text-xs text-blue-600 mb-3">
          <Bookmark className="w-3 h-3 fill-current" />
          <span>Saved</span>
        </div>
      )}
      {job.state === "applied" && (
        <div className="flex items-center gap-1 text-xs text-green-600 mb-3">
          <CheckCircle className="w-3 h-3" />
          <span>Applied</span>
        </div>
      )}

      {/* Header Row */}
      <div className="flex items-start justify-between gap-4 mb-3">
        <div className="flex-1">
          <h3 className="font-semibold text-lg text-gray-900 mb-1">
            {job.title}
          </h3>
          <div className="flex items-center gap-2 text-sm text-gray-600">
            <span className="font-medium">{job.company}</span>
            <span>•</span>
            <div className="flex items-center gap-1">
              <MapPin className="w-3 h-3" />
              <span>{job.location}</span>
            </div>
            <span>•</span>
            <Badge variant="outline" className={getWorkModeColor(job.workMode)}>
              {job.workMode}
            </Badge>
          </div>
        </div>

        {/* Match Score */}
        <div className="text-right shrink-0">
          <div
            className={`inline-flex items-center justify-center w-14 h-14 rounded-lg border font-semibold text-lg ${getMatchColor(
              job.matchScore
            )}`}
          >
            {job.matchScore}
          </div>
          <div className="text-xs text-gray-500 mt-1">{job.matchLabel}</div>
        </div>
      </div>

      {/* Description */}
      <p className="text-sm text-gray-600 mb-3 line-clamp-2">
        {job.description}
      </p>

      {/* Recommendation Explanation */}
      <div className="bg-blue-50 border border-blue-100 rounded px-3 py-2 mb-3">
        <p className="text-sm text-blue-900">{job.recommendationExplanation}</p>
      </div>

      {/* Tags */}
      <div className="flex flex-wrap gap-2 mb-4">
        {job.tags.map((tag) => (
          <Badge key={tag} variant="secondary" className="text-xs">
            {tag}
          </Badge>
        ))}
      </div>

      {/* Metadata */}
      <div className="flex items-center gap-3 text-xs text-gray-500 mb-4">
        <span>{job.postedDate}</span>
        {job.salary && (
          <>
            <span>•</span>
            <span className="text-gray-700 font-medium">{job.salary}</span>
          </>
        )}
        {job.source && (
          <>
            <span>•</span>
            <span className="font-medium text-gray-700">{job.source}</span>
          </>
        )}
      </div>

      {/* Actions */}
      <div className="flex items-center gap-2">
        {job.state === "dismissed" && onRecover ? (
          <Button
            variant="outline"
            size="sm"
            onClick={(e) => {
              e.stopPropagation();
              onRecover();
            }}
            className="gap-1"
          >
            <RotateCcw className="w-3.5 h-3.5" />
            Recover
          </Button>
        ) : (
          <>
            <Button
              variant="outline"
              size="sm"
              onClick={(e) => {
                e.stopPropagation();
                onSave?.();
              }}
              disabled={job.state === "saved"}
              className="gap-1"
            >
              <Bookmark className="w-3.5 h-3.5" />
              {job.state === "saved" ? "Saved" : "Save"}
            </Button>
            <Button
              size="sm"
              onClick={(e) => {
                e.stopPropagation();
                onApply?.();
              }}
              disabled={job.state === "applied"}
              className="gap-1"
            >
              <CheckCircle className="w-3.5 h-3.5" />
              {job.state === "applied" ? "Applied" : "Apply"}
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={(e) => {
                e.stopPropagation();
                onDismiss?.();
              }}
              disabled={job.state === "dismissed"}
              className="gap-1"
            >
              <X className="w-3.5 h-3.5" />
              Dismiss
            </Button>
          </>
        )}

        {/* Feedback placeholder */}
        <div className="ml-auto flex items-center gap-1 border-l border-gray-200 pl-3">
          <button
            onClick={(e) => e.stopPropagation()}
            className="p-1.5 hover:bg-gray-100 rounded transition-colors"
            aria-label="Thumbs up"
          >
            <ThumbsUp className="w-3.5 h-3.5 text-gray-400" />
          </button>
          <button
            onClick={(e) => e.stopPropagation()}
            className="p-1.5 hover:bg-gray-100 rounded transition-colors"
            aria-label="Thumbs down"
          >
            <ThumbsDown className="w-3.5 h-3.5 text-gray-400" />
          </button>
        </div>
      </div>
    </div>
  );
}
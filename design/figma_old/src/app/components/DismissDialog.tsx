import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "./ui/dialog";
import { Button } from "./ui/button";
import { Checkbox } from "./ui/checkbox";
import { Label } from "./ui/label";
import { Textarea } from "./ui/textarea";

interface DismissDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirm: (reason: string[]) => void;
  jobTitle: string;
}

export function DismissDialog({ open, onOpenChange, onConfirm, jobTitle }: DismissDialogProps) {
  const [selectedReasons, setSelectedReasons] = useState<string[]>([]);
  const [otherReason, setOtherReason] = useState("");

  const reasons = [
    "Not interested in the role",
    "Location doesn't work for me",
    "Salary is too low",
    "Looking for different seniority level",
    "Company culture concerns",
    "Missing required skills",
    "Not the right work mode (remote/hybrid/onsite)",
  ];

  const handleToggleReason = (reason: string) => {
    setSelectedReasons(prev =>
      prev.includes(reason)
        ? prev.filter(r => r !== reason)
        : [...prev, reason]
    );
  };

  const handleConfirm = () => {
    const allReasons = [...selectedReasons];
    if (otherReason.trim()) {
      allReasons.push(otherReason.trim());
    }
    onConfirm(allReasons);
    setSelectedReasons([]);
    setOtherReason("");
  };

  const handleCancel = () => {
    onOpenChange(false);
    setSelectedReasons([]);
    setOtherReason("");
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Dismiss job</DialogTitle>
          <DialogDescription>
            Help us understand why "{jobTitle}" isn't a good fit. This will improve future recommendations.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-3 py-4">
          {reasons.map((reason) => (
            <div key={reason} className="flex items-center space-x-2">
              <Checkbox
                id={reason}
                checked={selectedReasons.includes(reason)}
                onCheckedChange={() => handleToggleReason(reason)}
              />
              <Label
                htmlFor={reason}
                className="text-sm font-normal cursor-pointer"
              >
                {reason}
              </Label>
            </div>
          ))}

          <div className="pt-2">
            <Label htmlFor="other-reason" className="text-sm">
              Other reason (optional)
            </Label>
            <Textarea
              id="other-reason"
              placeholder="Tell us more..."
              value={otherReason}
              onChange={(e) => setOtherReason(e.target.value)}
              className="mt-2 min-h-20"
            />
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleCancel}>
            Cancel
          </Button>
          <Button onClick={handleConfirm}>
            Dismiss Job
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

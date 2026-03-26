import { useState } from "react";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Button } from "./ui/button";
import { Textarea } from "./ui/textarea";
import { Badge } from "./ui/badge";
import { User, Upload, Linkedin, Plus, X } from "lucide-react";

interface WorkExperience {
  id: string;
  title: string;
  company: string;
  duration: string;
  description: string;
}

export function ProfilePage() {
  const [linkedinUrl, setLinkedinUrl] = useState("");
  const [targetRoles, setTargetRoles] = useState<string[]>(["Product Manager", "Technical Product Manager"]);
  const [newTargetRole, setNewTargetRole] = useState("");
  const [preferredLocations, setPreferredLocations] = useState<string[]>(["San Francisco, CA", "New York, NY", "Remote"]);
  const [newLocation, setNewLocation] = useState("");
  const [workExperiences, setWorkExperiences] = useState<WorkExperience[]>([
    {
      id: "1",
      title: "Senior Product Manager",
      company: "TechCorp",
      duration: "2021 - Present",
      description: "Led product operations for platform team, scaling from 50 to 500 engineers."
    }
  ]);
  const [isAddingExperience, setIsAddingExperience] = useState(false);
  const [newExperience, setNewExperience] = useState<Omit<WorkExperience, 'id'>>({
    title: "",
    company: "",
    duration: "",
    description: ""
  });

  // Mock extracted data
  const skills = [
    "Product Management",
    "Product Operations",
    "Systems Thinking",
    "Data Analysis",
    "Stakeholder Management",
    "API Design",
    "Automation",
    "SQL",
  ];

  const experienceSummary = `7 years of product management experience with focus on platform products and operations. Led product operations at Series B startup, scaling from 50 to 500 engineers. Strong technical background with experience in API design, data systems, and developer tools.`;

  const handleAddTargetRole = () => {
    if (newTargetRole.trim() && !targetRoles.includes(newTargetRole.trim())) {
      setTargetRoles([...targetRoles, newTargetRole.trim()]);
      setNewTargetRole("");
    }
  };

  const handleRemoveTargetRole = (role: string) => {
    setTargetRoles(targetRoles.filter(r => r !== role));
  };

  const handleAddLocation = () => {
    if (newLocation.trim() && !preferredLocations.includes(newLocation.trim())) {
      setPreferredLocations([...preferredLocations, newLocation.trim()]);
      setNewLocation("");
    }
  };

  const handleRemoveLocation = (location: string) => {
    setPreferredLocations(preferredLocations.filter(l => l !== location));
  };

  const handleAddExperience = () => {
    if (newExperience.title.trim() && newExperience.company.trim()) {
      setWorkExperiences([
        ...workExperiences,
        { ...newExperience, id: Date.now().toString() }
      ]);
      setNewExperience({ title: "", company: "", duration: "", description: "" });
      setIsAddingExperience(false);
    }
  };

  const handleRemoveExperience = (id: string) => {
    setWorkExperiences(workExperiences.filter(exp => exp.id !== id));
  };

  return (
    <div className="h-screen overflow-y-auto">
      <div className="bg-white border-b border-gray-200 p-6">
        <div className="flex items-center gap-2">
          <User className="w-5 h-5 text-gray-600" />
          <h1 className="font-semibold text-xl text-gray-900">Profile</h1>
        </div>
      </div>

      <div className="p-6 max-w-3xl">
        <div className="space-y-8">
          {/* Profile Information */}
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <h2 className="font-semibold text-lg text-gray-900 mb-4">Profile Information</h2>
            <div className="space-y-4">
              <div>
                <Label htmlFor="resume">Resume</Label>
                <div className="mt-2 flex items-center gap-3">
                  <Button variant="outline" className="gap-2">
                    <Upload className="w-4 h-4" />
                    Upload Resume
                  </Button>
                  <span className="text-sm text-gray-500">PDF, DOC (max 5MB)</span>
                </div>
                <p className="text-xs text-gray-500 mt-2">
                  Current: resume_john_doe.pdf
                </p>
              </div>

              <div>
                <Label htmlFor="linkedin">LinkedIn Profile</Label>
                <div className="mt-2 relative">
                  <Linkedin className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
                  <Input
                    id="linkedin"
                    type="url"
                    placeholder="https://linkedin.com/in/yourprofile"
                    value={linkedinUrl}
                    onChange={(e) => setLinkedinUrl(e.target.value)}
                    className="pl-10"
                  />
                </div>
              </div>

              <div>
                <Label>Target Roles</Label>
                <div className="mt-2 flex flex-wrap gap-2 mb-2">
                  {targetRoles.map((role) => (
                    <Badge key={role} variant="secondary" className="gap-1 pr-1">
                      {role}
                      <button
                        onClick={() => handleRemoveTargetRole(role)}
                        className="ml-1 hover:bg-gray-300 rounded-full p-0.5"
                      >
                        <X className="w-3 h-3" />
                      </button>
                    </Badge>
                  ))}
                </div>
                <div className="flex gap-2">
                  <Input
                    placeholder="Add target role..."
                    value={newTargetRole}
                    onChange={(e) => setNewTargetRole(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        handleAddTargetRole();
                      }
                    }}
                  />
                  <Button
                    type="button"
                    variant="outline"
                    onClick={handleAddTargetRole}
                    className="gap-1"
                  >
                    <Plus className="w-4 h-4" />
                    Add
                  </Button>
                </div>
              </div>

              <div>
                <Label>Preferred Locations</Label>
                <div className="mt-2 flex flex-wrap gap-2 mb-2">
                  {preferredLocations.map((location) => (
                    <Badge key={location} variant="secondary" className="gap-1 pr-1">
                      {location}
                      <button
                        onClick={() => handleRemoveLocation(location)}
                        className="ml-1 hover:bg-gray-300 rounded-full p-0.5"
                      >
                        <X className="w-3 h-3" />
                      </button>
                    </Badge>
                  ))}
                </div>
                <div className="flex gap-2">
                  <Input
                    placeholder="Add location..."
                    value={newLocation}
                    onChange={(e) => setNewLocation(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        handleAddLocation();
                      }
                    }}
                  />
                  <Button
                    type="button"
                    variant="outline"
                    onClick={handleAddLocation}
                    className="gap-1"
                  >
                    <Plus className="w-4 h-4" />
                    Add
                  </Button>
                </div>
              </div>

              <Button>Save Changes</Button>
            </div>
          </div>

          {/* Work Experience */}
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="font-semibold text-lg text-gray-900">Work Experience</h2>
              {!isAddingExperience && (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setIsAddingExperience(true)}
                  className="gap-1"
                >
                  <Plus className="w-4 h-4" />
                  Add Experience
                </Button>
              )}
            </div>

            <div className="space-y-4">
              {workExperiences.map((exp) => (
                <div key={exp.id} className="border border-gray-200 rounded-lg p-4">
                  <div className="flex items-start justify-between mb-2">
                    <div>
                      <h3 className="font-medium text-gray-900">{exp.title}</h3>
                      <p className="text-sm text-gray-600">{exp.company}</p>
                      <p className="text-xs text-gray-500 mt-1">{exp.duration}</p>
                    </div>
                    <button
                      onClick={() => handleRemoveExperience(exp.id)}
                      className="text-gray-400 hover:text-gray-600"
                    >
                      <X className="w-4 h-4" />
                    </button>
                  </div>
                  {exp.description && (
                    <p className="text-sm text-gray-700 mt-2">{exp.description}</p>
                  )}
                </div>
              ))}

              {isAddingExperience && (
                <div className="border border-gray-300 rounded-lg p-4 bg-gray-50">
                  <div className="space-y-3">
                    <div>
                      <Label htmlFor="exp-title" className="text-sm">Job Title</Label>
                      <Input
                        id="exp-title"
                        value={newExperience.title}
                        onChange={(e) => setNewExperience({ ...newExperience, title: e.target.value })}
                        placeholder="e.g., Senior Product Manager"
                        className="mt-1"
                      />
                    </div>
                    <div>
                      <Label htmlFor="exp-company" className="text-sm">Company</Label>
                      <Input
                        id="exp-company"
                        value={newExperience.company}
                        onChange={(e) => setNewExperience({ ...newExperience, company: e.target.value })}
                        placeholder="e.g., Google"
                        className="mt-1"
                      />
                    </div>
                    <div>
                      <Label htmlFor="exp-duration" className="text-sm">Duration</Label>
                      <Input
                        id="exp-duration"
                        value={newExperience.duration}
                        onChange={(e) => setNewExperience({ ...newExperience, duration: e.target.value })}
                        placeholder="e.g., 2020 - 2023"
                        className="mt-1"
                      />
                    </div>
                    <div>
                      <Label htmlFor="exp-description" className="text-sm">Description (optional)</Label>
                      <Textarea
                        id="exp-description"
                        value={newExperience.description}
                        onChange={(e) => setNewExperience({ ...newExperience, description: e.target.value })}
                        placeholder="Brief description of your role and achievements..."
                        className="mt-1 min-h-20"
                      />
                    </div>
                    <div className="flex gap-2">
                      <Button onClick={handleAddExperience}>
                        Add Experience
                      </Button>
                      <Button
                        variant="outline"
                        onClick={() => {
                          setIsAddingExperience(false);
                          setNewExperience({ title: "", company: "", duration: "", description: "" });
                        }}
                      >
                        Cancel
                      </Button>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Extracted Data */}
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <h2 className="font-semibold text-lg text-gray-900 mb-4">
              Extracted Data
            </h2>
            <div className="space-y-4">
              <div>
                <Label className="text-sm font-medium text-gray-700 mb-2 block">
                  Skills
                </Label>
                <div className="flex flex-wrap gap-2">
                  {skills.map((skill) => (
                    <Badge key={skill} variant="secondary">
                      {skill}
                    </Badge>
                  ))}
                </div>
              </div>

              <div>
                <Label className="text-sm font-medium text-gray-700 mb-2 block">
                  Experience Summary
                </Label>
                <Textarea
                  value={experienceSummary}
                  readOnly
                  className="min-h-32 bg-gray-50"
                />
              </div>
            </div>
          </div>

          {/* Future Placeholder Sections */}
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <h2 className="font-semibold text-lg text-gray-900 mb-4">
              Match Optimization
            </h2>
            <div className="space-y-6">
              <div className="border border-dashed border-gray-300 rounded-lg p-6 text-center">
                <h3 className="font-medium text-gray-900 mb-1">Skill Gaps</h3>
                <p className="text-sm text-gray-500">
                  Identify skills to develop based on your target roles
                </p>
                <div className="text-xs text-gray-400 mt-2 italic">Coming soon</div>
              </div>

              <div className="border border-dashed border-gray-300 rounded-lg p-6 text-center">
                <h3 className="font-medium text-gray-900 mb-1">Resume Suggestions</h3>
                <p className="text-sm text-gray-500">
                  AI-powered recommendations to improve your resume
                </p>
                <div className="text-xs text-gray-400 mt-2 italic">Coming soon</div>
              </div>

              <div className="border border-dashed border-gray-300 rounded-lg p-6 text-center">
                <h3 className="font-medium text-gray-900 mb-1">Match Tuning</h3>
                <p className="text-sm text-gray-500">
                  Fine-tune your preferences to get better job matches
                </p>
                <div className="text-xs text-gray-400 mt-2 italic">Coming soon</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

"use client";

import React, { useEffect, useId, useState } from "react";
import { cn } from "@/lib/utils";
import { ChevronsUpDown, Check, Loader2 } from "lucide-react";
import { Button } from "./ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "./ui/popover";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "./ui/command";

interface ComboboxProps {
  label?: string;
  options?: string[];
  value: string;
  onChange: (val: string) => void;
  placeholder?: string;
  // controlled input value inside the popover search field
  inputValue?: string;
  // notify parent when the user types in the search field
  onInputChange?: (val: string) => void;
  // show loading indicator while options are being fetched
  loading?: boolean;
  // position of the label relative to the control
  labelPosition?: "top" | "bottom";
}

export default function Combobox({
  label,
  options = [],
  value,
  onChange,
  placeholder,
  inputValue,
  onInputChange,
  loading,
  labelPosition = "bottom",
}: ComboboxProps) {
  const [open, setOpen] = useState(false);
  const [internalInput, setInternalInput] = useState("");

  useEffect(() => {
    if (inputValue !== undefined) setInternalInput(inputValue);
  }, [inputValue]);

  const id = useId();

  const renderLabel = () =>
    label ? (
      <label htmlFor={`combobox-${id}`} className="text-sm text-gray-700 mt-1">
        {label}
      </label>
    ) : null;

  return (
    <div className="flex flex-col items-start">
      {labelPosition === "top" && renderLabel()}

      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            id={`combobox-${id}`}
            role="combobox"
            aria-expanded={open}
            className="w-[200px] justify-between"
            // always allow opening so user can search even if options currently empty
            disabled={false}
          >
            <div className="flex items-center gap-2">
              <span>{value || placeholder}</span>
              {loading ? (
                <Loader2 className="ml-2 h-4 w-4 animate-spin opacity-70" />
              ) : (
                <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
              )}
            </div>
          </Button>
        </PopoverTrigger>

        <PopoverContent className="w-[200px] p-0">
          <Command>
            <CommandInput
              placeholder={
                placeholder || `Search ${label?.toLowerCase() || "item"}...`
              }
              value={internalInput}
              onValueChange={(val: string) => {
                setInternalInput(val);
                onInputChange?.(val);
              }}
            />
            <CommandList>
              <CommandEmpty>No results found.</CommandEmpty>
              <CommandGroup>
                {options.map((opt) => (
                  <CommandItem
                    key={opt}
                    value={opt}
                    onSelect={(currentValue) => {
                      onChange(currentValue === value ? "" : currentValue);
                      setOpen(false);
                    }}
                  >
                    <Check
                      className={cn(
                        "mr-2 h-4 w-4",
                        value === opt ? "opacity-100" : "opacity-0"
                      )}
                    />
                    {opt}
                  </CommandItem>
                ))}
              </CommandGroup>
            </CommandList>
          </Command>
        </PopoverContent>
      </Popover>

      {labelPosition === "bottom" && renderLabel()}
    </div>
  );
}

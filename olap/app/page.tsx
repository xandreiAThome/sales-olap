'use client';

import useSWR from 'swr';
import {
  PieChart, Pie, Cell, Tooltip, ResponsiveContainer, Legend
} from 'recharts';
import { useState } from 'react';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { fetcher } from '@/utils/fetcher';
import SliceDiv from '@/components/slice';
import DiceDiv from '@/components/dice';
import RollupDiv from '@/components/rollup';

export default function Home() {
  const [city, setCity] = useState('East Kobe');
  const [fetchKey, setFetchKey] = useState<string | null>(null); // 🔹 only fetch when this changes

  const { data: sliceData, error: sliceError, isLoading } = useSWR(fetchKey, fetcher);

  const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444'];

  const handleGenerate = () => {
    if (!city.trim()) return;
    setFetchKey(`http://localhost:4000/api/slice/${encodeURIComponent(city.trim())}`);
  };

  return (
    <div className="flex flex-col justify-center min-h-screen p-24 bg-gray-100 gap-16">
      {/* <SliceDiv /> */}
      {/* <DiceDiv /> */}
      <RollupDiv />
    </div>
  );
}

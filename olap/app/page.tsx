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
  const { data: cities, error: citiesError } = useSWR(
      'http://localhost:4000/api/cities',
      fetcher
  );

  return (
    <div className="flex flex-col justify-center min-h-screen p-24 bg-gray-100 gap-16">
      {/* <RollupDiv /> */}
      {/* <SliceDiv /> */}
      <DiceDiv cities={cities}/>
      
    </div>
  );
}

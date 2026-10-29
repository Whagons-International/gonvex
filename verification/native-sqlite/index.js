import React, { useEffect, useState } from 'react';
import { Text, ScrollView, SafeAreaView } from 'react-native';
import { registerRootComponent } from 'expo';
import { execute } from './.work/proof.js';
import { collectorURL } from './.work/config.js';
function App() {
  const [state, setState] = useState('Starting actual Expo SQLite / Hermes verification');
  useEffect(() => {
    execute(collectorURL, event => setState(JSON.stringify(event, null, 2))).catch(async error => {
      const failed = {
        type: 'fatal',
        message: String(error),
        stack: error?.stack
      };
      setState(JSON.stringify(failed, null, 2));
      await fetch(collectorURL + '/event', {
        method: 'POST',
        headers: {
          'content-type': 'application/json'
        },
        body: JSON.stringify(failed)
      }).catch(() => {});
    });
  }, []);
  return <SafeAreaView style={{
    flex: 1,
    backgroundColor: '#f5f5f5'
  }}><ScrollView contentContainerStyle={{
      padding: 24
    }}><Text style={{
        fontSize: 22,
        fontWeight: '600',
        marginBottom: 16
      }}>Gonvex SQLite verification</Text><Text selectable style={{
        fontFamily: 'monospace',
        fontSize: 14
      }}>{state}</Text></ScrollView></SafeAreaView>;
}
registerRootComponent(App);

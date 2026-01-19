import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  TextField,
  Button,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  CircularProgress,
  Alert,
  Paper,
} from '@mui/material';
import { Send, SmartToy, Person, Lightbulb } from '@mui/icons-material';

import { getSuggestedQuestions, startConversation, sendMessage } from '../services/api';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  sql?: string;
}

const AIAssistant: React.FC = () => {
  const [input, setInput] = useState('');
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [suggestions, setSuggestions] = useState<{ name: string; questions: string[] }[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadSuggestions();
  }, []);

  const loadSuggestions = async () => {
    try {
      const res = await getSuggestedQuestions();
      setSuggestions(res.data.categories || []);
    } catch (err) {
      console.error('Failed to load suggestions:', err);
    }
  };

  const handleSend = async (messageText?: string) => {
    const text = messageText || input;
    if (!text.trim()) return;

    setInput('');
    setError(null);
    setMessages((prev) => [...prev, { role: 'user', content: text }]);
    setLoading(true);

    try {
      let convId = conversationId;

      // Start new conversation if needed
      if (!convId) {
        const spaceId = ''; // This would come from config
        if (!spaceId) {
          throw new Error('Genie Space not configured. Please configure GENIE_SPACE_ID in settings.');
        }
        const convRes = await startConversation(spaceId, text);
        convId = convRes.data.conversation_id;
        setConversationId(convId);
      }

      // Send message
      const msgRes = await sendMessage(convId!, text);

      const assistantMessage: Message = {
        role: 'assistant',
        content: msgRes.data.content || 'I processed your request.',
        sql: msgRes.data.sql,
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (err: any) {
      console.error('Failed to send message:', err);
      setError(err.message || 'Failed to get response from AI assistant.');
      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          content: 'Sorry, I encountered an error processing your request. Please try again.',
        },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const handleSuggestionClick = (question: string) => {
    handleSend(question);
  };

  return (
    <Box sx={{ height: 'calc(100vh - 150px)', display: 'flex', gap: 3 }}>
      {/* Main Chat Area */}
      <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
        <Typography variant="h4" fontWeight={600} gutterBottom>
          AI Assistant
        </Typography>

        {error && (
          <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
            {error}
          </Alert>
        )}

        {/* Messages */}
        <Card sx={{ flex: 1, mb: 2, overflow: 'auto' }}>
          <CardContent>
            {messages.length === 0 ? (
              <Box sx={{ textAlign: 'center', py: 4 }}>
                <SmartToy sx={{ fontSize: 64, color: 'text.secondary', mb: 2 }} />
                <Typography variant="h6" color="text.secondary" gutterBottom>
                  Ask me about your jobs and costs
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  I can help you analyze job performance, cost attribution, and more.
                  Try one of the suggested questions to get started.
                </Typography>
              </Box>
            ) : (
              <List>
                {messages.map((msg, index) => (
                  <ListItem
                    key={index}
                    sx={{
                      flexDirection: 'column',
                      alignItems: msg.role === 'user' ? 'flex-end' : 'flex-start',
                    }}
                  >
                    <Box
                      sx={{
                        display: 'flex',
                        alignItems: 'flex-start',
                        gap: 1,
                        maxWidth: '80%',
                      }}
                    >
                      {msg.role === 'assistant' && (
                        <SmartToy sx={{ color: 'primary.main', mt: 0.5 }} />
                      )}
                      <Paper
                        sx={{
                          p: 2,
                          bgcolor: msg.role === 'user' ? 'primary.main' : 'background.paper',
                          border: msg.role === 'assistant' ? '1px solid' : 'none',
                          borderColor: 'divider',
                        }}
                      >
                        <Typography variant="body1" sx={{ whiteSpace: 'pre-wrap' }}>
                          {msg.content}
                        </Typography>
                        {msg.sql && (
                          <Box
                            sx={{
                              mt: 2,
                              p: 1,
                              bgcolor: '#1E1E1E',
                              borderRadius: 1,
                              fontFamily: 'monospace',
                              fontSize: '0.85rem',
                              overflow: 'auto',
                            }}
                          >
                            <Typography variant="caption" color="text.secondary">
                              SQL Query:
                            </Typography>
                            <pre style={{ margin: 0, color: '#E0E0E0' }}>{msg.sql}</pre>
                          </Box>
                        )}
                      </Paper>
                      {msg.role === 'user' && (
                        <Person sx={{ color: 'primary.main', mt: 0.5 }} />
                      )}
                    </Box>
                  </ListItem>
                ))}
                {loading && (
                  <ListItem>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <SmartToy sx={{ color: 'primary.main' }} />
                      <CircularProgress size={20} />
                      <Typography variant="body2" color="text.secondary">
                        Thinking...
                      </Typography>
                    </Box>
                  </ListItem>
                )}
              </List>
            )}
          </CardContent>
        </Card>

        {/* Input */}
        <Box sx={{ display: 'flex', gap: 1 }}>
          <TextField
            fullWidth
            placeholder="Ask a question about your jobs, costs, or performance..."
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && !loading && handleSend()}
            disabled={loading}
          />
          <Button
            variant="contained"
            onClick={() => handleSend()}
            disabled={loading || !input.trim()}
            sx={{ minWidth: 100 }}
          >
            <Send />
          </Button>
        </Box>
      </Box>

      {/* Suggestions Sidebar */}
      <Card sx={{ width: 300, overflow: 'auto' }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
            <Lightbulb color="primary" />
            <Typography variant="h6">Suggested Questions</Typography>
          </Box>

          {suggestions.map((category) => (
            <Box key={category.name} sx={{ mb: 3 }}>
              <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                {category.name}
              </Typography>
              <List dense>
                {category.questions.map((question, index) => (
                  <ListItemButton
                    key={index}
                    onClick={() => handleSuggestionClick(question)}
                    sx={{ borderRadius: 1, mb: 0.5 }}
                    disabled={loading}
                  >
                    <ListItemText
                      primary={question}
                      primaryTypographyProps={{ variant: 'body2' }}
                    />
                  </ListItemButton>
                ))}
              </List>
            </Box>
          ))}
        </CardContent>
      </Card>
    </Box>
  );
};

export default AIAssistant;

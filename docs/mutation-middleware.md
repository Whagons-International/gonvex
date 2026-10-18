# Mutation middleware

Apps may install one `UseMutationMiddleware` callback during `Register`. It
receives the request context, function path, raw JSON arguments and `MutationNext`.
It may remove request-only metadata before calling `next` and observe its result
or error. Keep app-specific diagnostic schemas and logging policies in the app.

The server calls `RunMutation` outside context creation, argument validation and
the database transaction. The callback therefore observes context failures,
permission errors and failed commits as well as handler outcomes. Rejected
WebSocket authentication also runs the callback with an `unauthenticated`
dispatch error. Both individual and batched mutations use this path.

`RuntimeContext.RequestID` contains the transport mutation ID. `User` and
`TenantID` come from the server's caller and routing context, never request
arguments. Unauthenticated attempts have no trusted user ID.

Direct SDK `ExecuteMutation` calls also run middleware before typed argument
decoding. The host's context marker avoids a second invocation for the inner
dispatch. Apps without middleware retain their existing behavior. Internal
scheduled mutations do not run public-request middleware.

Middleware may recover panics and return a safe error. The host always defers
transaction rollback so panic recovery cannot leave an open transaction.
Middleware must not log raw arguments, credentials or arbitrary error messages.

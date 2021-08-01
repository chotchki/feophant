The layers of the can be visualized roughly in this form at the moment.

<pre>
                Table                   Table                    Table                     Table                Uuid
┌────────────┐    +     ┌────────────┐    +     ┌─────────────┐    +     ┌──────────────┐    +     ┌──────────┐   +   ┌──────────┐
│            │ SqlTuple │            │ SqlTuple │             │ SqlTuple │              │ RowData  │          │ Page  │          │
│  Trigger   │ ───────► │  Security  │ ───────► │  Constraint │ ───────► │  Visible Row │ ───────► │  Row     │ ────► │  I/O     │
│            │          │            │          │             │          │              │          │          │       │          │
│  Manager   │ ◄─────── │  Manager   │ ◄─────── │  Manager    │ ◄─────── │  Manager     │ ◄─────── │  Manager │ ◄──── │  Manager │
│            │ SqlTuple │            │ SqlTuple │             │ SqlTuple │              │ RowData  │          │ Uuid  │          │
└────────────┘    +     └────────────┘    +     └─────────────┘    +     └──────────────┘    +     └──────────┘   +   └──────────┘
                Type                    Type           ▲         Type           ▲          Type                 Page
                                                       │                        │

                                                     Null                   Transaction
                                                     Unique                 Manager
                                                     Custom
</pre>

[comment]: # (Diagram made in AsciiFlow!)
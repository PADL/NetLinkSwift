#pragma once

// include glibc's netinet/in.h and net/if.h first so the kernel's libc-compat.h
// suppresses the duplicate in6_addr/ifreq the netfilter uapi headers would
// otherwise redefine (and clash with CNetLink over)
#include <netinet/in.h>
#include <net/if.h>

#include <libmnl/libmnl.h>

#include <libnftnl/common.h>
#include <libnftnl/table.h>
#include <libnftnl/chain.h>
#include <libnftnl/rule.h>
#include <libnftnl/expr.h>

#include <linux/netfilter.h>
#include <linux/netfilter_bridge.h>
#include <linux/netfilter/nfnetlink.h>
#include <linux/netfilter/nf_tables.h>

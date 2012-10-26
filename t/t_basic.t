
use Test::More;
use Test::Cmd;
use strict;


my $bin = "/kb/deployment/bin";
my $test = Test::Cmd->new(prog => "$bin/jkbase", workdir => '');

ok($test, "creating Test::Cmd object");

$test->run(args => "fs -ls");
ok($? == 0, "fs ls");
ok($test->stdout =~ /Found/, "found output");

$test->run(args => "compute list_jobs");
ok($? == 0, "compute list_jobs");

if ($test->stdout =~ /ThriftJobStatus.*job_id:(.*?),/)
{
    my $job = $1;
    ok($test->stdout =~ /ThriftJobStatus/, "found job status");
    $test->run(args => "compute status -job $job");
    ok($? == 0, "job_status $job");
    ok($test->stdout =~ /Username/, "compute job_status contents");
}

done_testing();

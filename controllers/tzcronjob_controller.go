/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	cronjobberv1alpha1 "github.com/hiddeco/cronjobber/api/v1alpha1"
	"github.com/robfig/cron"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	batchv1listers "k8s.io/client-go/listers/batch/v1"
	"k8s.io/client-go/tools/record"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// TZCronJobReconciler reconciles a TZCronJob object
type TZCronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
	now func() time.Time

	recorder   record.EventRecorder
	jobLister  batchv1listers.JobLister
	jobControl jobControlInterface
	queue      workqueue.RateLimitingInterface
}

/*
We'll mock out the clock to make it easier to jump around in time while testing,
the "real" clock just calls `time.Now`.
*/
type realClock struct{}

func (_ realClock) Now() time.Time { return time.Now() }

// clock knows how to get the current time.
// It can be used to fake out timing for testing.
type Clock interface {
	Now() time.Time
}

//+kubebuilder:rbac:groups=cronjobber.hidde.co,resources=tzcronjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=cronjobber.hidde.co,resources=tzcronjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=cronjobber.hidde.co,resources=tzcronjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

var (
	scheduledTimeAnnotation = "hidde.co.cronjobber/scheduled-at"
	nextScheduleDelta       = 100 * time.Millisecond
)

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the TZCronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.11.0/pkg/reconcile
func (jm *TZCronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var cronJob cronjobberv1alpha1.TZCronJob
	if err := jm.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		log.Error(err, "unable to fetch CronJob")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	requeueAfter, err := jm.sync(ctx, req.NamespacedName, cronJob.GetName())
	switch {
	case err != nil:
		utilruntime.HandleError(fmt.Errorf("error syncing CronJobController %v, requeuing: %v", cronJob.GetName(), err))
		jm.queue.AddRateLimited(cronJob)
	case requeueAfter != nil:
		jm.queue.Forget(cronJob)
		jm.queue.AddAfter(cronJob, *requeueAfter)
	}
	return ctrl.Result{Requeue: true, RequeueAfter: *requeueAfter}, nil
	// /*
	// 	### 2: List all active jobs, and update the status
	// 	To fully update our status, we'll need to list all child jobs in this namespace that belong to this CronJob.
	// 	Similarly to Get, we can use the List method to list the child jobs.  Notice that we use variadic options to
	// 	set the namespace and field match (which is actually an index lookup that we set up below).
	// */
	// var childJobs kbatch.JobList
	// if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
	// 	log.Error(err, "unable to list child Jobs")
	// 	return ctrl.Result{}, err
	// }
	// /*
	// 	<aside class="note">
	// 	<h1>What is this index about?</h1>
	// 	<p>The reconciler fetches all jobs owned by the cronjob for the status. As our number of cronjobs increases,
	// 	looking these up can become quite slow as we have to filter through all of them. For a more efficient lookup,
	// 	these jobs will be indexed locally on the controller's name. A jobOwnerKey field is added to the
	// 	cached job objects. This key references the owning controller and functions as the index. Later in this
	// 	document we will configure the manager to actually index this field.</p>
	// 	</aside>
	// 	Once we have all the jobs we own, we'll split them into active, successful,
	// 	and failed jobs, keeping track of the most recent run so that we can record it
	// 	in status.  Remember, status should be able to be reconstituted from the state
	// 	of the world, so it's generally not a good idea to read from the status of the
	// 	root object.  Instead, you should reconstruct it every run.  That's what we'll
	// 	do here.
	// 	We can check if a job is "finished" and whether it succeeded or failed using status
	// 	conditions.  We'll put that logic in a helper to make our code cleaner.
	// */

	// // find the active list of jobs
	// var activeJobs []*kbatch.Job
	// var successfulJobs []*kbatch.Job
	// var failedJobs []*kbatch.Job
	// var mostRecentTime *time.Time // find the last run so we can update the status

	// /*
	// 	We consider a job "finished" if it has a "Complete" or "Failed" condition marked as true.
	// 	Status conditions allow us to add extensible status information to our objects that other
	// 	humans and controllers can examine to check things like completion and health.
	// */
	// isJobFinished := func(job *kbatch.Job) (bool, kbatch.JobConditionType) {
	// 	for _, c := range job.Status.Conditions {
	// 		if (c.Type == kbatch.JobComplete || c.Type == kbatch.JobFailed) && c.Status == corev1.ConditionTrue {
	// 			return true, c.Type
	// 		}
	// 	}

	// 	return false, ""
	// }
	// /*
	// 	We'll use a helper to extract the scheduled time from the annotation that
	// 	we added during job creation.
	// */
	// getScheduledTimeForJob := func(job *kbatch.Job) (*time.Time, error) {
	// 	timeRaw := job.Annotations[scheduledTimeAnnotation]
	// 	if len(timeRaw) == 0 {
	// 		return nil, nil
	// 	}

	// 	timeParsed, err := time.Parse(time.RFC3339, timeRaw)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return &timeParsed, nil
	// }

	// for i, job := range childJobs.Items {
	// 	_, finishedType := isJobFinished(&job)
	// 	switch finishedType {
	// 	case "": // ongoing
	// 		activeJobs = append(activeJobs, &childJobs.Items[i])
	// 	case kbatch.JobFailed:
	// 		failedJobs = append(failedJobs, &childJobs.Items[i])
	// 	case kbatch.JobComplete:
	// 		successfulJobs = append(successfulJobs, &childJobs.Items[i])
	// 	}

	// 	// We'll store the launch time in an annotation, so we'll reconstitute that from
	// 	// the active jobs themselves.
	// 	scheduledTimeForJob, err := getScheduledTimeForJob(&job)
	// 	if err != nil {
	// 		log.Error(err, "unable to parse schedule time for child job", "job", &job)
	// 		continue
	// 	}
	// 	if scheduledTimeForJob != nil {
	// 		if mostRecentTime == nil {
	// 			mostRecentTime = scheduledTimeForJob
	// 		} else if mostRecentTime.Before(*scheduledTimeForJob) {
	// 			mostRecentTime = scheduledTimeForJob
	// 		}
	// 	}
	// }

	// if mostRecentTime != nil {
	// 	cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	// } else {
	// 	cronJob.Status.LastScheduleTime = nil
	// }
	// cronJob.Status.Active = nil
	// for _, activeJob := range activeJobs {
	// 	jobRef, err := ref.GetReference(r.Scheme, activeJob)
	// 	if err != nil {
	// 		log.Error(err, "unable to make reference to active job", "job", activeJob)
	// 		continue
	// 	}
	// 	cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	// }

	// /*
	// 	Here, we'll log how many jobs we observed at a slightly higher logging level,
	// 	for debugging.  Notice how instead of using a format string, we use a fixed message,
	// 	and attach key-value pairs with the extra information.  This makes it easier to
	// 	filter and query log lines.
	// */
	// log.V(1).Info("job count", "active jobs", len(activeJobs), "successful jobs", len(successfulJobs), "failed jobs", len(failedJobs))

	// /*
	// 	Using the date we've gathered, we'll update the status of our CRD.
	// 	Just like before, we use our client.  To specifically update the status
	// 	subresource, we'll use the `Status` part of the client, with the `Update`
	// 	method.
	// 	The status subresource ignores changes to spec, so it's less likely to conflict
	// 	with any other updates, and can have separate permissions.
	// */
	// if err := r.Status().Update(ctx, &cronJob); err != nil {
	// 	log.Error(err, "unable to update CronJob status")
	// 	return ctrl.Result{}, err
	// }

	// /*
	// 	Once we've updated our status, we can move on to ensuring that the status of
	// 	the world matches what we want in our spec.
	// 	### 3: Clean up old jobs according to the history limit
	// 	First, we'll try to clean up old jobs, so that we don't leave too many lying
	// 	around.
	// */

	// // NB: deleting these is "best effort" -- if we fail on a particular one,
	// // we won't requeue just to finish the deleting.
	// if cronJob.Spec.FailedJobsHistoryLimit != nil {
	// 	sort.Slice(failedJobs, func(i, j int) bool {
	// 		if failedJobs[i].Status.StartTime == nil {
	// 			return failedJobs[j].Status.StartTime != nil
	// 		}
	// 		return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
	// 	})
	// 	for i, job := range failedJobs {
	// 		if int32(i) >= int32(len(failedJobs))-*cronJob.Spec.FailedJobsHistoryLimit {
	// 			break
	// 		}
	// 		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
	// 			log.Error(err, "unable to delete old failed job", "job", job)
	// 		} else {
	// 			log.V(0).Info("deleted old failed job", "job", job)
	// 		}
	// 	}
	// }

	// if cronJob.Spec.SuccessfulJobsHistoryLimit != nil {
	// 	sort.Slice(successfulJobs, func(i, j int) bool {
	// 		if successfulJobs[i].Status.StartTime == nil {
	// 			return successfulJobs[j].Status.StartTime != nil
	// 		}
	// 		return successfulJobs[i].Status.StartTime.Before(successfulJobs[j].Status.StartTime)
	// 	})
	// 	for i, job := range successfulJobs {
	// 		if int32(i) >= int32(len(successfulJobs))-*cronJob.Spec.SuccessfulJobsHistoryLimit {
	// 			break
	// 		}
	// 		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); (err) != nil {
	// 			log.Error(err, "unable to delete old successful job", "job", job)
	// 		} else {
	// 			log.V(0).Info("deleted old successful job", "job", job)
	// 		}
	// 	}
	// }

	// /* ### 4: Check if we're suspended
	// If this object is suspended, we don't want to run any jobs, so we'll stop now.
	// This is useful if something's broken with the job we're running and we want to
	// pause runs to investigate or putz with the cluster, without deleting the object.
	// */

	// if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
	// 	log.V(1).Info("cronjob suspended, skipping")
	// 	return ctrl.Result{}, nil
	// }

	// /*
	// 	### 5: Get the next scheduled run
	// 	If we're not paused, we'll need to calculate the next scheduled run, and whether
	// 	or not we've got a run that we haven't processed yet.
	// */

	// /*
	// 	We'll calculate the next scheduled time using our helpful cron library.
	// 	We'll start calculating appropriate times from our last run, or the creation
	// 	of the CronJob if we can't find a last run.
	// 	If there are too many missed runs and we don't have any deadlines set, we'll
	// 	bail so that we don't cause issues on controller restarts or wedges.
	// 	Otherwise, we'll just return the missed runs (of which we'll just use the latest),
	// 	and the next run, so that we can know when it's time to reconcile again.
	// */
	// getNextSchedule := func(cronJob *cronjobberv1alpha1.TZCronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
	// 	sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
	// 	if err != nil {
	// 		return time.Time{}, time.Time{}, fmt.Errorf("unparseable schedule %q: %v", cronJob.Spec.Schedule, err)
	// 	}

	// 	// for optimization purposes, cheat a bit and start from our last observed run time
	// 	// we could reconstitute this here, but there's not much point, since we've
	// 	// just updated it.
	// 	var earliestTime time.Time
	// 	if cronJob.Status.LastScheduleTime != nil {
	// 		earliestTime = cronJob.Status.LastScheduleTime.Time.In(now.Location())
	// 	} else {
	// 		earliestTime = cronJob.ObjectMeta.CreationTimestamp.Time.In(now.Location())
	// 	}
	// 	if cronJob.Spec.StartingDeadlineSeconds != nil {
	// 		// controller is not going to schedule anything below this point
	// 		schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds)).In(now.Location())

	// 		if schedulingDeadline.After(earliestTime) {
	// 			earliestTime = schedulingDeadline
	// 		}
	// 	}
	// 	if earliestTime.After(now) {
	// 		return time.Time{}, sched.Next(now), nil
	// 	}

	// 	starts := 0
	// 	for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
	// 		lastMissed = t
	// 		// An object might miss several starts. For example, if
	// 		// controller gets wedged on Friday at 5:01pm when everyone has
	// 		// gone home, and someone comes in on Tuesday AM and discovers
	// 		// the problem and restarts the controller, then all the hourly
	// 		// jobs, more than 80 of them for one hourly scheduledJob, should
	// 		// all start running with no further intervention (if the scheduledJob
	// 		// allows concurrency and late starts).
	// 		//
	// 		// However, if there is a bug somewhere, or incorrect clock
	// 		// on controller's server or apiservers (for setting creationTimestamp)
	// 		// then there could be so many missed start times (it could be off
	// 		// by decades or more), that it would eat up all the CPU and memory
	// 		// of this controller. In that case, we want to not try to list
	// 		// all the missed start times.
	// 		starts++
	// 		if starts > 100 {
	// 			// We can't get the most recent times so just return an empty slice
	// 			return time.Time{}, time.Time{}, fmt.Errorf("too many missed start times (> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew")
	// 		}
	// 	}
	// 	return lastMissed, sched.Next(now), nil
	// }

	// now, err := getCurrentTimeInZone(&cronJob)
	// if err != nil {
	// 	log.Error(err, "unable to convert time into required timezone CronJob schedule")
	// 	// we don't really care about requeuing until we get an update that
	// 	// fixes the schedule, so don't return an error
	// 	return ctrl.Result{}, nil
	// }

	// // figure out the next times that we need to create
	// // jobs at (or anything we missed).
	// missedRun, nextRun, err := getNextSchedule(&cronJob, now)
	// if err != nil {
	// 	log.Error(err, "unable to figure out CronJob schedule")
	// 	// we don't really care about requeuing until we get an update that
	// 	// fixes the schedule, so don't return an error
	// 	return ctrl.Result{}, nil
	// }

	// /*
	// 	We'll prep our eventual request to requeue until the next job, and then figure
	// 	out if we actually need to run.
	// */
	// scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(now)} // save this so we can re-use it elsewhere
	// log = log.WithValues("now", now, "next run", nextRun)

	// /*
	// 	### 6: Run a new job if it's on schedule, not past the deadline, and not blocked by our concurrency policy
	// 	If we've missed a run, and we're still within the deadline to start it, we'll need to run a job.
	// */
	// if missedRun.IsZero() {
	// 	log.V(1).Info("no upcoming scheduled times, sleeping until next")
	// 	return scheduledResult, nil
	// }

	// // make sure we're not too late to start the run
	// log = log.WithValues("current run", missedRun)
	// tooLate := false
	// if cronJob.Spec.StartingDeadlineSeconds != nil {
	// 	tooLate = missedRun.Add(time.Duration(*cronJob.Spec.StartingDeadlineSeconds) * time.Second).Before(now)
	// }
	// if tooLate {
	// 	log.V(1).Info("missed starting deadline for last run, sleeping till next")
	// 	// TODO(directxman12): events
	// 	return scheduledResult, nil
	// }

	// /*
	// 	If we actually have to run a job, we'll need to either wait till existing ones finish,
	// 	replace the existing ones, or just add new ones.  If our information is out of date due
	// 	to cache delay, we'll get a requeue when we get up-to-date information.
	// */
	// // figure out how to run this job -- concurrency policy might forbid us from running
	// // multiple at the same time...
	// if cronJob.Spec.ConcurrencyPolicy == cronjobberv1alpha1.ForbidConcurrent && len(activeJobs) > 0 {
	// 	log.V(1).Info("concurrency policy blocks concurrent runs, skipping", "num active", len(activeJobs))
	// 	return scheduledResult, nil
	// }

	// // ...or instruct us to replace existing ones...
	// if cronJob.Spec.ConcurrencyPolicy == cronjobberv1alpha1.ReplaceConcurrent {
	// 	for _, activeJob := range activeJobs {
	// 		// we don't care if the job was already deleted
	// 		if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
	// 			log.Error(err, "unable to delete active job", "job", activeJob)
	// 			return ctrl.Result{}, err
	// 		}
	// 	}
	// }

	// /*
	// 	Once we've figured out what to do with existing jobs, we'll actually create our desired job
	// */

	// /*
	// 	We need to construct a job based on our CronJob's template.  We'll copy over the spec
	// 	from the template and copy some basic object meta.
	// 	Then, we'll set the "scheduled time" annotation so that we can reconstitute our
	// 	`LastScheduleTime` field each reconcile.
	// 	Finally, we'll need to set an owner reference.  This allows the Kubernetes garbage collector
	// 	to clean up jobs when we delete the CronJob, and allows controller-runtime to figure out
	// 	which cronjob needs to be reconciled when a given job changes (is added, deleted, completes, etc).
	// */
	// constructJobForCronJob := func(cronJob *cronjobberv1alpha1.TZCronJob, scheduledTime time.Time) (*kbatch.Job, error) {
	// 	// We want job names for a given nominal start time to have a deterministic name to avoid the same job being created twice
	// 	name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())

	// 	job := &kbatch.Job{
	// 		ObjectMeta: metav1.ObjectMeta{
	// 			Labels:      make(map[string]string),
	// 			Annotations: make(map[string]string),
	// 			Name:        name,
	// 			Namespace:   cronJob.Namespace,
	// 		},
	// 		Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
	// 	}
	// 	for k, v := range cronJob.Spec.JobTemplate.Annotations {
	// 		job.Annotations[k] = v
	// 	}
	// 	job.Annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)
	// 	for k, v := range cronJob.Spec.JobTemplate.Labels {
	// 		job.Labels[k] = v
	// 	}
	// 	if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
	// 		return nil, err
	// 	}

	// 	return job, nil
	// }

	// // actually make the job...
	// job, err := constructJobForCronJob(&cronJob, missedRun)
	// if err != nil {
	// 	log.Error(err, "unable to construct job from template")
	// 	// don't bother requeuing until we get a change to the spec
	// 	return scheduledResult, nil
	// }

	// // ...and create it on the cluster
	// if err := r.Create(ctx, job); err != nil {
	// 	log.Error(err, "unable to create Job for CronJob", "job", job)
	// 	return ctrl.Result{}, err
	// }

	// log.V(1).Info("created Job for CronJob run", "job", job)

	// /*
	// 	### 7: Requeue when we either see a running job or it's time for the next scheduled run
	// 	Finally, we'll return the result that we prepped above, that says we want to requeue
	// 	when our next run would need to occur.  This is taken as a maximum deadline -- if something
	// 	else changes in between, like our job starts or finishes, we get modified, etc, we might
	// 	reconcile again sooner.
	// */
	// // we'll requeue once we see the running job, and update our status
	//return scheduledResult, nil
}

/*
### Setup
Finally, we'll update our setup.  In order to allow our reconciler to quickly
look up Jobs by their owner, we'll need an index.  We declare an index key that
we can later use with the client as a pseudo-field name, and then describe how to
extract the indexed value from the Job object.  The indexer will automatically take
care of namespaces for us, so we just have to extract the owner name if the Job has
a CronJob owner.
Additionally, we'll inform the manager that this controller owns some Jobs, so that it
will automatically call Reconcile on the underlying CronJob when a Job changes, is
deleted, etc.
*/
var (
	jobOwnerKey = ".metadata.controller"
	apiGVStr    = cronjobberv1alpha1.GroupVersion.String()
)

// SetupWithManager sets up the controller with the Manager.
func (r *TZCronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// set up a real clock, since we're not in a test
	if r.Clock == nil {
		r.Clock = realClock{}
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &batchv1.Job{}, jobOwnerKey, func(rawObj client.Object) []string {
		// grab the job object, extract the owner...
		job := rawObj.(*batchv1.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		// ...make sure it's a CronJob...
		if owner.APIVersion != apiGVStr || owner.Kind != "TZCronJob" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&cronjobberv1alpha1.TZCronJob{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

func (jm *TZCronJobReconciler) sync(ctx context.Context, ns types.NamespacedName, name string) (*time.Duration, error) {
	var cronJob cronjobberv1alpha1.TZCronJob

	err := jm.Get(ctx, ns, &cronJob)
	switch {
	case errors.IsNotFound(err):
		// may be cronjob is deleted, don't need to requeue this key
		klog.V(4).InfoS("TZCronJob not found, may be it is deleted", "cronjob", klog.KRef(ns.Name, name), "err", err)
		return nil, nil
	case err != nil:
		// for other transient apiserver error requeue with exponential backoff
		return nil, err
	}

	var jobsToBeReconciled batchv1.JobList
	if err := jm.List(ctx, &jobsToBeReconciled, client.InNamespace(ns.Name), client.MatchingFields{jobOwnerKey: name}); err != nil {
		klog.V(4).InfoS("unable to list child Jobs")
		return nil, err
	}

	cronJobCopy, requeueAfter, updateStatus, err := jm.syncCronJob(ctx, &cronJob, &jobsToBeReconciled, ns)
	if err != nil {
		klog.V(2).InfoS("Error reconciling cronjob", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()), "err", err)
		if updateStatus {
			if err := jm.Status().Update(ctx, cronJobCopy); err != nil {
				klog.V(2).InfoS("Unable to update status for cronjob", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()), "resourceVersion", cronJob.ResourceVersion, "err", err)
				return nil, err
			}
		}
		return nil, err
	}

	if jm.cleanupFinishedJobs(ctx, cronJobCopy, &jobsToBeReconciled) {
		updateStatus = true
	}

	// Update the CronJob if needed
	if updateStatus {
		if err := jm.Status().Update(ctx, cronJobCopy); err != nil {
			klog.V(2).InfoS("Unable to update status for cronjob", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()), "resourceVersion", cronJob.ResourceVersion, "err", err)
			return nil, err
		}
	}

	if requeueAfter != nil {
		klog.V(4).InfoS("Re-queuing cronjob", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()), "requeueAfter", requeueAfter)
		return requeueAfter, nil
	}
	// this marks the key done, currently only happens when the cronjob is suspended or spec has invalid schedule format
	return nil, nil
}

func (jm *TZCronJobReconciler) syncCronJob(
	ctx context.Context,
	cronJob *cronjobberv1alpha1.TZCronJob,
	jobs *batchv1.JobList,
	ns types.NamespacedName) (*cronjobberv1alpha1.TZCronJob, *time.Duration, bool, error) {

	cronJob = cronJob.DeepCopy()
	now := jm.now()
	updateStatus := false

	childrenJobs := make(map[types.UID]bool)
	for _, j := range jobs.Items {
		childrenJobs[j.ObjectMeta.UID] = true
		found := inActiveList(cronJob, j.ObjectMeta.UID)
		if !found && !IsJobFinished(&j) {
			var cjCopy cronjobberv1alpha1.TZCronJob
			err := jm.Get(ctx, ns, &cjCopy)
			if err != nil {
				return nil, nil, updateStatus, err
			}
			if inActiveList(&cjCopy, j.ObjectMeta.UID) {
				cronJob = &cjCopy
				continue
			}
			//	jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "UnexpectedJob", "Saw a job that the controller did not create or forgot: %s", j.Name)
			// We found an unfinished job that has us as the parent, but it is not in our Active list.
			// This could happen if we crashed right after creating the Job and before updating the status,
			// or if our jobs list is newer than our cj status after a relist, or if someone intentionally created
			// a job that they wanted us to adopt.
		} else if found && IsJobFinished(&j) {
			_, _ = getFinishedStatus(&j)
			deleteFromActiveList(cronJob, j.ObjectMeta.UID)
			//jm.recorder.Eventf(cronJob, corev1.EventTypeNormal, "SawCompletedJob", "Saw completed job: %s, status: %v", j.Name, status)
			updateStatus = true
		} else if IsJobFinished(&j) {
			// a job does not have to be in active list, as long as it is finished, we will process the timestamp
			if cronJob.Status.LastSuccessfulTime == nil {
				cronJob.Status.LastSuccessfulTime = j.Status.CompletionTime
				updateStatus = true
			}
			if j.Status.CompletionTime != nil && j.Status.CompletionTime.After(cronJob.Status.LastSuccessfulTime.Time) {
				cronJob.Status.LastSuccessfulTime = j.Status.CompletionTime
				updateStatus = true
			}
		}
	}

	// Remove any job reference from the active list if the corresponding job does not exist any more.
	// Otherwise, the cronjob may be stuck in active mode forever even though there is no matching
	// job running.
	for _, j := range cronJob.Status.Active {
		_, found := childrenJobs[j.UID]
		if found {
			continue
		}
		// Explicitly try to get the job from api-server to avoid a slow watch not able to update
		// the job lister on time, giving an unwanted miss
		_, err := jm.jobControl.GetJob(j.Namespace, j.Name)
		switch {
		case errors.IsNotFound(err):
			// The job is actually missing, delete from active list and schedule a new one if within
			// deadline
			//jm.recorder.Eventf(cronJob, corev1.EventTypeNormal, "MissingJob", "Active job went missing: %v", j.Name)
			deleteFromActiveList(cronJob, j.UID)
			updateStatus = true
		case err != nil:
			return cronJob, nil, updateStatus, err
		}
		// the job is missing in the lister but found in api-server
	}

	if cronJob.DeletionTimestamp != nil {
		// The CronJob is being deleted.
		// Don't do anything other than updating status.
		return cronJob, nil, updateStatus, nil
	}

	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		klog.V(4).InfoS("Not starting job because the cron is suspended", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()))
		return cronJob, nil, updateStatus, nil
	}

	sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
	if err != nil {
		// this is likely a user error in defining the spec value
		// we should log the error and not reconcile this cronjob until an update to spec
		klog.V(2).InfoS("Unparseable schedule", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()), "schedule", cronJob.Spec.Schedule, "err", err)
		//	jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "UnparseableSchedule", "unparseable schedule: %q : %s", cronJob.Spec.Schedule, err)
		return cronJob, nil, updateStatus, nil
	}

	if strings.Contains(cronJob.Spec.Schedule, "TZ") {
		//	jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "UnsupportedSchedule", "CRON_TZ or TZ used in schedule %q is not officially supported, see https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/ for more details", cronJob.Spec.Schedule)
	}

	scheduledTime, err := getNextScheduleTime(*cronJob, now, sched, jm.recorder)
	if err != nil {
		// this is likely a user error in defining the spec value
		// we should log the error and not reconcile this cronjob until an update to spec
		klog.V(2).InfoS("invalid schedule", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()), "schedule", cronJob.Spec.Schedule, "err", err)
		//	jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "InvalidSchedule", "invalid schedule: %s : %s", cronJob.Spec.Schedule, err)
		return cronJob, nil, updateStatus, nil
	}
	if scheduledTime == nil {
		// no unmet start time, return cj,.
		// The only time this should happen is if queue is filled after restart.
		// Otherwise, the queue is always suppose to trigger sync function at the time of
		// the scheduled time, that will give atleast 1 unmet time schedule
		klog.V(4).InfoS("No unmet start times", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()))
		t := nextScheduledTimeDuration(sched, now)
		return cronJob, t, updateStatus, nil
	}

	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		tooLate = scheduledTime.Add(time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds)).Before(now)
	}
	if tooLate {
		klog.V(4).InfoS("Missed starting window", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()))
		//	jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "MissSchedule", "Missed scheduled time to start a job: %s", scheduledTime.UTC().Format(time.RFC1123Z))

		// TODO: Since we don't set LastScheduleTime when not scheduling, we are going to keep noticing
		// the miss every cycle.  In order to avoid sending multiple events, and to avoid processing
		// the cj again and again, we could set a Status.LastMissedTime when we notice a miss.
		// Then, when we call getRecentUnmetScheduleTimes, we can take max(creationTimestamp,
		// Status.LastScheduleTime, Status.LastMissedTime), and then so we won't generate
		// and event the next time we process it, and also so the user looking at the status
		// can see easily that there was a missed execution.
		t := nextScheduledTimeDuration(sched, now)
		return cronJob, t, updateStatus, nil
	}
	if isJobInActiveList(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getJobName(cronJob, *scheduledTime),
			Namespace: cronJob.Namespace,
		}}, cronJob.Status.Active) || cronJob.Status.LastScheduleTime.Equal(&metav1.Time{Time: *scheduledTime}) {
		klog.V(4).InfoS("Not starting job because the scheduled time is already processed", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()), "schedule", scheduledTime)
		t := nextScheduledTimeDuration(sched, now)
		return cronJob, t, updateStatus, nil
	}
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(cronJob.Status.Active) > 0 {
		// Regardless which source of information we use for the set of active jobs,
		// there is some risk that we won't see an active job when there is one.
		// (because we haven't seen the status update to the SJ or the created pod).
		// So it is theoretically possible to have concurrency with Forbid.
		// As long the as the invocations are "far enough apart in time", this usually won't happen.
		//
		// TODO: for Forbid, we could use the same name for every execution, as a lock.
		// With replace, we could use a name that is deterministic per execution time.
		// But that would mean that you could not inspect prior successes or failures of Forbid jobs.
		klog.V(4).InfoS("Not starting job because prior execution is still running and concurrency policy is Forbid", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()))
		jm.recorder.Eventf(cronJob, corev1.EventTypeNormal, "JobAlreadyActive", "Not starting job because prior execution is running and concurrency policy is Forbid")
		t := nextScheduledTimeDuration(sched, now)
		return cronJob, t, updateStatus, nil
	}
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, j := range cronJob.Status.Active {
			klog.V(4).InfoS("Deleting job that was still running at next scheduled start time", "job", klog.KRef(j.Namespace, j.Name))
			job, err := jm.jobControl.GetJob(j.Namespace, j.Name)
			if err != nil {
				jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "FailedGet", "Get job: %v", err)
				return cronJob, nil, updateStatus, err
			}
			if !deleteJob(cronJob, job, jm.jobControl, jm.recorder) {
				return cronJob, nil, updateStatus, fmt.Errorf("could not replace job %s/%s", job.Namespace, job.Name)
			}
			updateStatus = true
		}
	}

	jobReq, err := getJobFromTemplate2(cronJob, *scheduledTime)
	if err != nil {
		klog.ErrorS(err, "Unable to make Job from template", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()))
		return cronJob, nil, updateStatus, err
	}
	jobResp, err := jm.jobControl.CreateJob(cronJob.Namespace, jobReq)
	switch {
	case errors.HasStatusCause(err, corev1.NamespaceTerminatingCause):
	case errors.IsAlreadyExists(err):
		// If the job is created by other actor, assume  it has updated the cronjob status accordingly
		klog.InfoS("Job already exists", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()), "job", klog.KRef(jobReq.GetNamespace(), jobReq.GetName()))
		return cronJob, nil, updateStatus, err
	case err != nil:
		// default error handling
		jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "FailedCreate", "Error creating job: %v", err)
		return cronJob, nil, updateStatus, err
	}

	//	metrics.CronJobCreationSkew.Observe(jobResp.ObjectMeta.GetCreationTimestamp().Sub(*scheduledTime).Seconds())
	klog.V(4).InfoS("Created Job", "job", klog.KRef(jobResp.GetNamespace(), jobResp.GetName()), "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()))
	jm.recorder.Eventf(cronJob, corev1.EventTypeNormal, "SuccessfulCreate", "Created job %v", jobResp.Name)

	// ------------------------------------------------------------------ //

	// If this process restarts at this point (after posting a job, but
	// before updating the status), then we might try to start the job on
	// the next time.  Actually, if we re-list the SJs and Jobs on the next
	// iteration of syncAll, we might not see our own status update, and
	// then post one again.  So, we need to use the job name as a lock to
	// prevent us from making the job twice (name the job with hash of its
	// scheduled time).

	// Add the just-started job to the status list.
	jobRef, err := getRef(jobResp)
	if err != nil {
		klog.V(2).InfoS("Unable to make object reference", "cronjob", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()), "err", err)
		return cronJob, nil, updateStatus, fmt.Errorf("unable to make object reference for job for %s", klog.KRef(cronJob.GetNamespace(), cronJob.GetName()))
	}
	cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	cronJob.Status.LastScheduleTime = &metav1.Time{Time: *scheduledTime}
	updateStatus = true

	t := nextScheduledTimeDuration(sched, now)
	return cronJob, t, updateStatus, nil
}

func (jm *TZCronJobReconciler) cleanupFinishedJobs(ctx context.Context, cj *cronjobberv1alpha1.TZCronJob, js *batchv1.JobList) bool {
	// If neither limits are active, there is no need to do anything.
	if cj.Spec.FailedJobsHistoryLimit == nil && cj.Spec.SuccessfulJobsHistoryLimit == nil {
		return false
	}
	updateStatus := false
	failedJobs := []*batchv1.Job{}
	successfulJobs := []*batchv1.Job{}

	for _, job := range js.Items {
		isFinished, finishedStatus := jm.getFinishedStatus(&job)
		if isFinished && finishedStatus == batchv1.JobComplete {
			successfulJobs = append(successfulJobs, &job)
		} else if isFinished && finishedStatus == batchv1.JobFailed {
			failedJobs = append(failedJobs, &job)
		}
	}

	if cj.Spec.SuccessfulJobsHistoryLimit != nil &&
		jm.removeOldestJobs(cj,
			successfulJobs,
			*cj.Spec.SuccessfulJobsHistoryLimit) {
		updateStatus = true
	}

	if cj.Spec.FailedJobsHistoryLimit != nil &&
		jm.removeOldestJobs(cj,
			failedJobs,
			*cj.Spec.FailedJobsHistoryLimit) {
		updateStatus = true
	}

	return updateStatus
}
func (jm *TZCronJobReconciler) getFinishedStatus(j *batchv1.Job) (bool, batchv1.JobConditionType) {
	for _, c := range j.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}
	return false, ""
}

// removeOldestJobs removes the oldest jobs from a list of jobs
func (jm *TZCronJobReconciler) removeOldestJobs(cj *cronjobberv1alpha1.TZCronJob, js []*batchv1.Job, maxJobs int32) bool {
	updateStatus := false
	numToDelete := len(js) - int(maxJobs)
	if numToDelete <= 0 {
		return updateStatus
	}

	klog.V(4).InfoS("Cleaning up jobs from CronJob list", "deletejobnum", numToDelete, "jobnum", len(js), "cronjob", klog.KRef(cj.GetNamespace(), cj.GetName()))

	sort.Sort(byJobStartTimeStar(js))
	for i := 0; i < numToDelete; i++ {
		klog.V(4).InfoS("Removing job from CronJob list", "job", js[i].Name, "cronjob", klog.KRef(cj.GetNamespace(), cj.GetName()))
		if deleteJob(cj, js[i], jm.jobControl, jm.recorder) {
			updateStatus = true
		}
	}
	return updateStatus
}

// deleteJob reaps a job, deleting the job, the pods and the reference in the active list
func deleteJob(cj *cronjobberv1alpha1.TZCronJob, job *batchv1.Job, jc jobControlInterface, recorder record.EventRecorder) bool {
	nameForLog := fmt.Sprintf("%s/%s", cj.Namespace, cj.Name)

	// delete the job itself...
	if err := jc.DeleteJob(job.Namespace, job.Name); err != nil {
		recorder.Eventf(cj, corev1.EventTypeWarning, "FailedDelete", "Deleted job: %v", err)
		klog.Errorf("Error deleting job %s from %s: %v", job.Name, nameForLog, err)
		return false
	}
	// ... and its reference from active list
	deleteFromActiveList(cj, job.ObjectMeta.UID)
	recorder.Eventf(cj, corev1.EventTypeNormal, "SuccessfulDelete", "Deleted job %v", job.Name)

	return true
}

func getRef(object runtime.Object) (*corev1.ObjectReference, error) {
	return ref.GetReference(scheme.Scheme, object)
}

// isJobInActiveList take a job and checks if activeJobs has a job with the same
// name and namespace.
func isJobInActiveList(job *batchv1.Job, activeJobs []corev1.ObjectReference) bool {
	for _, j := range activeJobs {
		if j.Name == job.Name && j.Namespace == job.Namespace {
			return true
		}
	}
	return false
}

func nextScheduledTimeDuration(sched cron.Schedule, now time.Time) *time.Duration {
	t := sched.Next(now).Add(nextScheduleDelta).Sub(now)
	return &t
}
func getJobName(cj *cronjobberv1alpha1.TZCronJob, scheduledTime time.Time) string {
	return fmt.Sprintf("%s-%d", cj.Name, getTimeHashInMinutes(scheduledTime))
}
